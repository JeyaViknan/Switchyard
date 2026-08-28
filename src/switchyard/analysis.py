"""Reading a configuration: what it validates as, and what it means.

Two jobs, both derived from the configuration file and -- where noted -- from
measurements taken against it. Nothing here invents a threshold or a number.

`review()` answers "is this configuration valid and internally consistent?".
It runs no services and is what `switchyard check` reports. It covers what
`GatewayConfig.validate()` cannot: validate rejects a configuration outright,
while these are combinations that load fine and still will not do what their
author expects.

`interpret()` answers "what does this configuration actually do?". A floor of
six slots is not a quantity anyone can reason about directly; the same floor
expressed as "sustains about 3.2 requests per second at the 1.9 seconds per
request we measured" is. These are arithmetic on the configuration, not
judgements about it, so they carry no PASS or FAIL -- a configuration is
entitled to mean whatever its author intended.

Anything that cannot be derived reliably says so rather than guessing.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from switchyard.core.config import GatewayConfig
from switchyard.scenarios.base import Check

UNKNOWN = "not enough data to say"


@dataclass(frozen=True, slots=True)
class Interpretation:
    """One derived statement about what the configuration does."""

    subject: str
    meaning: str
    caveat: str = ""


# -- validation ------------------------------------------------------------


def review(config: GatewayConfig) -> list[Check]:
    """Static checks. No services, no load, no timing."""
    checks: list[Check] = []
    reserved = sum(t.reserved_concurrency for t in config.tenants)
    shared = config.max_concurrency - reserved

    checks.append(Check.result(
        "capacity is fully allocatable", shared >= 0,
        f"{reserved} reserved of {config.max_concurrency}, {shared} shared",
        "reserved floors add up to more capacity than the gateway has",
        "lower reserved_concurrency, or raise gateway.max_concurrency",
    ))

    if not config.tenants:
        checks.append(Check.skip(
            "tenants are isolated from each other",
            "no tenants configured; the gateway is in open development mode",
        ))
        checks.append(Check.skip("no tenant can take the whole gateway", "no tenants configured"))
        checks.append(Check.result(
            "operational endpoints are protected", True, "open development mode"
        ))
        return checks

    floored = [t for t in config.tenants if t.reserved_concurrency > 0]
    if len(config.tenants) < 2:
        checks.append(Check.skip(
            "tenants are isolated from each other",
            "only one tenant is configured, so there is nobody to be isolated from",
        ))
    elif not floored:
        checks.append(Check.skip(
            "tenants are isolated from each other",
            "no tenant sets reserved_concurrency, so no isolation is claimed",
            "give latency-sensitive tenants a reserved_concurrency floor: weight "
            "divides contended capacity but does not stop them queueing behind a flood",
        ))
    else:
        checks.append(Check.result(
            "tenants are isolated from each other", True,
            f"{len(floored)} of {len(config.tenants)} tenants have a reserved floor",
        ))

    unbounded = [t.id for t in config.tenants if t.max_concurrency is None]
    checks.append(Check.result(
        "no tenant can take the whole gateway", not unbounded,
        f"{len(config.tenants) - len(unbounded)} of {len(config.tenants)} have a ceiling",
        f"{', '.join(unbounded)} can occupy all shared capacity when others are idle",
        "set max_concurrency on each tenant",
    ))

    starved = [
        f"{t.id} (floor {t.reserved_concurrency} > ceiling {t.max_concurrency})"
        for t in config.tenants
        if t.max_concurrency is not None and t.reserved_concurrency > t.max_concurrency
    ]
    if starved:
        checks.append(Check.result(
            "floors fit inside their ceilings", False, "; ".join(starved),
            "a tenant could never reach the floor its configuration promises",
            "raise max_concurrency or lower reserved_concurrency",
        ))

    zero_budget = [t.id for t in config.tenants if t.budget_tokens == 0]
    if zero_budget:
        checks.append(Check.result(
            "budgets allow at least one request", False,
            f"{', '.join(zero_budget)} have budget_tokens = 0",
            "these tenants are configured but can never be served",
            "raise budget_tokens, or remove it for an unlimited budget",
        ))

    checks.append(Check.result(
        "operational endpoints are protected", bool(config.admin_key_sha256),
        "admin key configured" if config.admin_key_sha256 else "no admin key set",
        "metrics, scheduler stats and drain would be unreachable",
        "run `switchyard keys mint --admin` and set gateway.admin_key_sha256",
    ))
    return checks


# -- interpretation --------------------------------------------------------


def interpret(config: GatewayConfig, service_s: float | None = None,
              output_tokens: float | None = None) -> list[Interpretation]:
    """What the configuration means, in units a person can act on.

    `service_s` and `output_tokens` come from measurement. Without them the
    capacity- and budget-related statements cannot be made, and are omitted
    rather than estimated.
    """
    out: list[Interpretation] = []
    reserved = sum(t.reserved_concurrency for t in config.tenants)
    shared = config.max_concurrency - reserved

    out.append(Interpretation(
        "capacity",
        f"{config.max_concurrency} concurrent requests: {reserved} reserved as floors, "
        f"{shared} shared between everyone",
    ))

    total_weight = sum(t.weight for t in config.tenants) or 1.0
    for t in config.tenants:
        parts = [f"floor {t.reserved_concurrency}"]
        if t.max_concurrency is not None:
            parts.append(f"ceiling {t.max_concurrency}")
        parts.append(f"{t.weight / total_weight:.0%} of contended capacity")

        meaning = ", ".join(parts)
        caveat = ""
        if service_s and t.reserved_concurrency:
            sustains = t.reserved_concurrency / service_s
            meaning += f" -- its floor alone sustains about {sustains:.1f} req/s"
            caveat = f"at the {service_s:.1f}s per request measured here"
        elif t.reserved_concurrency == 0:
            caveat = "no floor, so it queues behind a busy neighbour"
        out.append(Interpretation(f"tenant '{t.id}'", meaning, caveat))

        if t.budget_tokens:
            if output_tokens:
                requests = t.budget_tokens / output_tokens
                out.append(Interpretation(
                    "  budget",
                    f"{t.budget_tokens:,} tokens is about {requests:,.0f} more requests",
                    f"at the {output_tokens:.0f} tokens per response measured here",
                ))
            else:
                out.append(Interpretation(
                    "  budget", f"{t.budget_tokens:,} tokens", UNKNOWN + " how long that lasts"
                ))

    for model, candidates in config.routes.items():
        if len(candidates) > 1:
            out.append(Interpretation(
                f"model '{model}'",
                f"tries {candidates[0]}, falls back to {' then '.join(candidates[1:])}",
            ))
        else:
            out.append(Interpretation(
                f"model '{model}'", f"only {candidates[0]}",
                "no fallback: a failure here reaches the client",
            ))

    needed = breaker_failures_needed(config)
    out.append(Interpretation(
        "circuit breaker",
        f"opens after about {needed} failures within the last {config.breaker.window}",
        f"then waits {config.breaker.cooldown_s:g}s before probing, doubling on repeat trips",
    ))

    out.append(Interpretation(
        "shutdown",
        f"refuses queued work at once and waits up to "
        f"{config.drain_timeout_s:g}s for running requests",
        "requests still running after that are abandoned"
        if config.drain_timeout_s < 30 else "",
    ))
    return out


def breaker_failures_needed(config: GatewayConfig) -> int:
    """Failures required before the breaker can trip, from its own settings."""
    return max(
        config.breaker.min_samples,
        int(config.breaker.window * config.breaker.failure_threshold),
    )


def render_interpretation(items: Sequence[Interpretation], style, width: int = 18) -> list[str]:
    lines = []
    for item in items:
        lines.append(f"    {item.subject:<{width}}{item.meaning}")
        if item.caveat:
            lines.append(f"    {'':<{width}}{style.dim(item.caveat)}")
    return lines

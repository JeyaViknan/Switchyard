"""Tenant and gateway configuration.

Tenants live in a TOML file rather than a database. They are configuration --
edited deliberately, reviewed, and deployed -- not runtime data, and there is no
admin API that would need to write them. A database here would add a failure
domain, a migration story, and an async query on the request path in exchange
for nothing the scheduler needs. When tenants become self-service, this moves.

Budgets are denominated in tokens, not currency. Tokens are the resource the
scheduler actually allocates; a per-model price is attached for reporting so
cost is still visible without the gateway carrying a pricing subsystem whose
numbers would be stale the week after they were written.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = "switchyard.toml"


class ConfigError(ValueError):
    """Raised for a malformed configuration, with the offending field named."""


@dataclass(frozen=True, slots=True)
class Tenant:
    """One tenant's identity and its share of the gateway's capacity.

    `weight` governs the share of *contended* capacity a tenant receives when
    several are backlogged. `reserved_concurrency` is a floor it can always
    reach regardless of what anyone else is doing, and `max_concurrency` is a
    ceiling that stops one tenant from occupying the whole gateway even when it
    is otherwise idle. The three together are what make a noisy neighbour
    survivable: a floor nobody can take, a ceiling nobody can exceed, and a
    weighted split of whatever is left.
    """

    id: str
    key_sha256: str
    weight: float = 1.0
    reserved_concurrency: int = 0
    max_concurrency: int | None = None
    budget_tokens: int | None = None
    max_queue_depth: int = 128
    deadline_s: float = 60.0
    max_tokens_cap: int = 4096

    def validate(self) -> None:
        if not self.id or not self.id.replace("-", "").replace("_", "").isalnum():
            raise ConfigError(f"tenant id {self.id!r} must be alphanumeric (- and _ allowed)")
        if len(self.key_sha256) != 64:
            raise ConfigError(f"tenant {self.id}: key_sha256 must be a 64-char hex digest")
        if self.weight <= 0:
            raise ConfigError(f"tenant {self.id}: weight must be > 0")
        if self.reserved_concurrency < 0:
            raise ConfigError(f"tenant {self.id}: reserved_concurrency must be >= 0")
        if self.max_concurrency is not None:
            if self.max_concurrency < 1:
                raise ConfigError(f"tenant {self.id}: max_concurrency must be >= 1")
            if self.max_concurrency < self.reserved_concurrency:
                raise ConfigError(
                    f"tenant {self.id}: max_concurrency ({self.max_concurrency}) is below "
                    f"reserved_concurrency ({self.reserved_concurrency}); the tenant could "
                    f"never reach its own guarantee"
                )
        if self.budget_tokens is not None and self.budget_tokens < 0:
            raise ConfigError(f"tenant {self.id}: budget_tokens must be >= 0")
        if self.max_queue_depth < 0:
            raise ConfigError(f"tenant {self.id}: max_queue_depth must be >= 0")
        if self.deadline_s <= 0:
            raise ConfigError(f"tenant {self.id}: deadline_s must be > 0")


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    """Four deadlines, because "slow" is not one failure mode.

    A single overall timeout cannot tell a provider that never answered from one
    that answered promptly and then stalled, and it makes both wait the full
    duration before anyone notices. Splitting them turns each into a distinct,
    quickly-detected, differently-handled failure.

    `ttft_s` is the important one. A failure before the first token has reached
    the client can be retried on another provider completely invisibly; after
    the first token it cannot. Keeping this deadline tight deliberately pushes
    failure mass into the window where recovery is still transparent.
    """

    connect_s: float = 2.0
    ttft_s: float = 8.0
    inter_token_s: float = 10.0
    total_s: float = 300.0

    def validate(self) -> None:
        for name in ("connect_s", "ttft_s", "inter_token_s", "total_s"):
            if getattr(self, name) <= 0:
                raise ConfigError(f"timeouts.{name} must be > 0")
        if self.total_s < self.ttft_s:
            raise ConfigError(
                f"timeouts.total_s ({self.total_s}) is below ttft_s ({self.ttft_s}): "
                f"the overall deadline would fire before a slow first token could"
            )


@dataclass(frozen=True, slots=True)
class BreakerConfig:
    """Circuit-breaker tuning, mirrored from core.health.BreakerPolicy."""

    failure_threshold: float = 0.5
    min_samples: int = 10
    window: int = 50
    cooldown_s: float = 5.0
    max_cooldown_s: float = 60.0
    jitter: float = 0.3
    half_open_probes: int = 2


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    """Gateway-wide settings.

    `max_concurrency` is the total number of requests that may be in flight to
    providers at once. It is the scarce resource the whole scheduler exists to
    allocate: set it to what the upstream can actually absorb, not to what the
    gateway can hold open.
    """

    max_concurrency: int = 32
    scheduling_policy: str = "drr"
    tenants: tuple[Tenant, ...] = ()
    # Credential for the operational endpoints: /metrics, /v1/providers,
    # /v1/scheduler/stats and /v1/admin/*. Separate from tenant keys because
    # these expose every tenant's usage and can take the gateway out of service,
    # which is not something one tenant should be able to do to the others.
    admin_key_sha256: str | None = None
    fleet_url: str = "http://127.0.0.1:8100"
    providers: tuple[str, ...] = ("fast", "slow")
    timeouts: TimeoutPolicy = TimeoutPolicy()
    breaker: BreakerConfig = BreakerConfig()
    # How long shutdown waits for in-flight requests before abandoning them.
    drain_timeout_s: float = 30.0
    # Ordered failover candidates per model. A model with no entry maps to the
    # provider of the same name, so single-provider setups need no routes at all.
    routes: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def validate(self) -> None:
        if self.max_concurrency < 1:
            raise ConfigError("gateway.max_concurrency must be >= 1")
        if self.drain_timeout_s < 0:
            raise ConfigError("gateway.drain_timeout_s must be >= 0")
        if self.admin_key_sha256 is not None and len(self.admin_key_sha256) != 64:
            raise ConfigError("gateway.admin_key_sha256 must be a 64-char hex digest")
        self.timeouts.validate()
        from switchyard.core.health import BreakerPolicy

        BreakerPolicy(**{f: getattr(self.breaker, f) for f in BreakerConfig.__slots__}).validate()
        for model, candidates in self.routes.items():
            if not candidates:
                raise ConfigError(f"routes.{model} lists no providers")
            unknown = [c for c in candidates if c not in self.providers]
            if unknown:
                raise ConfigError(
                    f"routes.{model} names provider(s) {unknown} that are not in "
                    f"gateway.providers {list(self.providers)}"
                )
        if self.scheduling_policy not in ("drr", "fifo"):
            raise ConfigError(
                f"gateway.scheduling_policy must be 'drr' or 'fifo', "
                f"got {self.scheduling_policy!r}"
            )
        seen: set[str] = set()
        for tenant in self.tenants:
            tenant.validate()
            if tenant.id in seen:
                raise ConfigError(f"duplicate tenant id {tenant.id!r}")
            seen.add(tenant.id)

        # Reserved capacity that exceeds the total is not a guarantee, it is an
        # overdraft: several tenants would each be promised a floor the gateway
        # cannot simultaneously honour. Better to refuse the config than to
        # discover it under load.
        reserved = sum(t.reserved_concurrency for t in self.tenants)
        if reserved > self.max_concurrency:
            raise ConfigError(
                f"reserved concurrency across tenants ({reserved}) exceeds "
                f"gateway.max_concurrency ({self.max_concurrency}): these "
                f"guarantees cannot all be honoured at once"
            )

    @property
    def tenants_by_id(self) -> dict[str, Tenant]:
        return {t.id: t for t in self.tenants}


def _tenant_from_toml(raw: dict[str, Any]) -> Tenant:
    known = {f for f in Tenant.__slots__}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"tenant {raw.get('id', '?')!r}: unknown field(s) {sorted(unknown)}. "
            f"Known fields: {sorted(known)}"
        )
    try:
        return Tenant(**raw)
    except TypeError as exc:
        raise ConfigError(f"tenant {raw.get('id', '?')!r}: {exc}") from exc


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> GatewayConfig:
    """Load and validate configuration. Raises ConfigError with a usable message."""
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"config file not found: {p}")

    with p.open("rb") as fh:
        raw = tomllib.load(fh)

    gateway_raw = dict(raw.get("gateway", {}))
    tenants = tuple(_tenant_from_toml(dict(t)) for t in raw.get("tenants", []))

    known = {"max_concurrency", "scheduling_policy", "fleet_url", "providers",
             "drain_timeout_s", "admin_key_sha256"}
    unknown = set(gateway_raw) - known
    if unknown:
        raise ConfigError(f"[gateway]: unknown field(s) {sorted(unknown)}")
    if "providers" in gateway_raw:
        gateway_raw["providers"] = tuple(gateway_raw["providers"])

    timeouts_raw = dict(raw.get("timeouts", {}))
    unknown = set(timeouts_raw) - set(TimeoutPolicy.__slots__)
    if unknown:
        raise ConfigError(f"[timeouts]: unknown field(s) {sorted(unknown)}")
    if timeouts_raw:
        gateway_raw["timeouts"] = TimeoutPolicy(**timeouts_raw)

    breaker_raw = dict(raw.get("breaker", {}))
    unknown = set(breaker_raw) - set(BreakerConfig.__slots__)
    if unknown:
        raise ConfigError(f"[breaker]: unknown field(s) {sorted(unknown)}")
    if breaker_raw:
        gateway_raw["breaker"] = BreakerConfig(**breaker_raw)

    routes_raw = raw.get("routes", {})
    if routes_raw:
        gateway_raw["routes"] = {
            model: tuple(candidates) for model, candidates in routes_raw.items()
        }

    config = GatewayConfig(tenants=tenants, **gateway_raw)
    config.validate()
    return config


def render_toml(config: GatewayConfig) -> str:
    """Serialise a configuration back to TOML.

    Used to run a copy of someone's configuration with test credentials
    substituted: `switchyard verify` needs raw keys to send traffic, and a
    configuration only stores digests. Everything that defines behaviour --
    capacity, weights, floors, ceilings, budgets, routes, timeouts, breaker
    settings -- is carried across unchanged, because those are the subject of
    the verification.
    """
    lines = [
        "[gateway]",
        f"max_concurrency = {config.max_concurrency}",
        f'scheduling_policy = "{config.scheduling_policy}"',
        f"drain_timeout_s = {config.drain_timeout_s}",
        "providers = [" + ", ".join(f'"{p}"' for p in config.providers) + "]",
    ]
    if config.admin_key_sha256:
        lines.append(f'admin_key_sha256 = "{config.admin_key_sha256}"')
    lines += [
        "",
        "[timeouts]",
        f"connect_s = {config.timeouts.connect_s}",
        f"ttft_s = {config.timeouts.ttft_s}",
        f"inter_token_s = {config.timeouts.inter_token_s}",
        f"total_s = {config.timeouts.total_s}",
        "",
        "[breaker]",
        f"failure_threshold = {config.breaker.failure_threshold}",
        f"min_samples = {config.breaker.min_samples}",
        f"window = {config.breaker.window}",
        f"cooldown_s = {config.breaker.cooldown_s}",
        f"max_cooldown_s = {config.breaker.max_cooldown_s}",
        f"jitter = {config.breaker.jitter}",
        f"half_open_probes = {config.breaker.half_open_probes}",
        "",
    ]
    if config.routes:
        lines.append("[routes]")
        for model, candidates in config.routes.items():
            lines.append(f"{model} = [" + ", ".join(f'"{c}"' for c in candidates) + "]")
        lines.append("")
    for t in config.tenants:
        lines += [
            "[[tenants]]",
            f'id = "{t.id}"',
            f'key_sha256 = "{t.key_sha256}"',
            f"weight = {t.weight}",
            f"reserved_concurrency = {t.reserved_concurrency}",
            f"max_queue_depth = {t.max_queue_depth}",
            f"deadline_s = {t.deadline_s}",
            f"max_tokens_cap = {t.max_tokens_cap}",
        ]
        if t.max_concurrency is not None:
            lines.append(f"max_concurrency = {t.max_concurrency}")
        if t.budget_tokens is not None:
            lines.append(f"budget_tokens = {t.budget_tokens}")
        lines.append("")
    return "\n".join(lines)

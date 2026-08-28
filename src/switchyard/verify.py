"""`switchyard verify` -- does this configuration deliver what it promises?

The scenarios answer "can Switchyard survive this?". This answers a different
question: "does *my* configuration actually give me the guarantees I think I
configured?" -- which is the one you have before changing limits in production.

Every expectation is read out of the configuration rather than chosen here. A
tenant with `reserved_concurrency = 6` is claiming it can always reach six
slots; `max_concurrency` claims a ceiling that is never exceeded;
`budget_tokens` claims a spending bound; a route with two candidates claims
failover. Those claims are what get tested, so the verifier has no opinion about
what your limits should be -- only about whether they hold.

Two things it deliberately does not do. It never calls your real providers:
traffic goes to the synthetic fleet standing in for whatever your configuration
names, so verifying costs nothing and cannot disturb production. And it runs a
*copy* of your configuration with test credentials substituted, because a
configuration stores key digests and the verifier needs to send real traffic.
Everything that defines behaviour is carried across unchanged.

Checks are grouped into phases so that one period of load answers several
questions at once. Running each check in isolation would be cleaner to read and
far too slow to use.
"""

from __future__ import annotations

import asyncio
import contextlib
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path

import httpx

from switchyard.analysis import interpret, render_interpretation, review
from switchyard.bench.harness import Stack
from switchyard.bench.loadgen import LoadSpec, RequestRecord, run_load_detailed
from switchyard.bench.stats import percentiles
from switchyard.core.auth import mint_admin_key, mint_key
from switchyard.core.config import GatewayConfig, load_config, render_toml
from switchyard.scenarios.base import Check, Reporter, ScenarioResult

# Queue wait a protected tenant should not exceed while a neighbour floods.
# A tenant inside its reserved floor should not be queueing at all, so this is
# deliberately loose: it is meant to catch "the floor is not working", not to
# grade latency.
PROTECTED_QUEUE_WAIT_MS = 750.0


@dataclass(slots=True)
class Observed:
    """Peak state seen while load was running."""

    max_inflight: int = 0
    max_per_tenant: dict[str, int] = field(default_factory=dict)
    max_queue_per_tenant: dict[str, int] = field(default_factory=dict)

    def record(self, stats: dict) -> None:
        self.max_inflight = max(self.max_inflight, stats["inflight"])
        for name, t in stats["tenants"].items():
            self.max_per_tenant[name] = max(self.max_per_tenant.get(name, 0), t["inflight"])
            self.max_queue_per_tenant[name] = max(
                self.max_queue_per_tenant.get(name, 0), t["queued"]
            )


@dataclass(slots=True)
class Harness:
    """The running copy of the user's configuration, plus credentials for it."""

    stack: Stack
    config: GatewayConfig
    tenant_keys: dict[str, str]
    admin_key: str

    @property
    def admin_headers(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self.admin_key}"}

    async def stats(self, client: httpx.AsyncClient) -> dict:
        r = await client.get(f"{self.stack.gateway.base_url}/v1/scheduler/stats",
                             headers=self.admin_headers)
        return r.json()

    async def providers(self, client: httpx.AsyncClient) -> dict:
        r = await client.get(f"{self.stack.gateway.base_url}/v1/providers",
                             headers=self.admin_headers)
        return r.json()

    async def metrics(self, client: httpx.AsyncClient) -> str:
        r = await client.get(f"{self.stack.gateway.base_url}/metrics",
                             headers=self.admin_headers)
        return r.text


def derive_config(config: GatewayConfig) -> tuple[GatewayConfig, dict[str, str], str]:
    """Copy the configuration with credentials we can actually use."""
    tenant_keys: dict[str, str] = {}
    tenants = []
    for tenant in config.tenants:
        raw, digest = mint_key(tenant.id)
        tenant_keys[tenant.id] = raw
        tenants.append(replace(tenant, key_sha256=digest))
    admin_raw, admin_digest = mint_admin_key()
    derived = replace(config, tenants=tuple(tenants), admin_key_sha256=admin_digest)
    return derived, tenant_keys, admin_raw


async def register_providers(stack: Stack, providers: Sequence[str]) -> None:
    """Make the synthetic fleet answer to whatever the configuration names."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        for name in providers:
            await c.put(f"{stack.fleet.base_url}/control/profiles/{name}",
                        json={"faults": {"error_rate": 0.0}})


async def sample(harness: Harness, observed: Observed, duration_s: float,
                 every_s: float = 0.4) -> None:
    deadline = asyncio.get_running_loop().time() + duration_s
    async with httpx.AsyncClient(timeout=5.0) as c:
        while asyncio.get_running_loop().time() < deadline:
            with contextlib.suppress(httpx.HTTPError, ValueError, KeyError):
                observed.record(await harness.stats(c))
            await asyncio.sleep(every_s)


def load_for(harness: Harness, tenant_id: str, rate: float, duration_s: float,
             model: str, seed: int, max_tokens: int = 256):
    return run_load_detailed(LoadSpec(
        url=harness.stack.completions_url, rate=rate, duration_s=duration_s,
        model=model, tenants=(tenant_id,), seed=seed, max_tokens=max_tokens,
        api_key=harness.tenant_keys[tenant_id], request_timeout_s=90.0,
    ))


def service_seconds(records: Sequence[RequestRecord]) -> float:
    done = [r for r in records if r.ok and r.latency is not None]
    if not done:
        return 2.5                                   # nothing measured; assume typical
    return sum((r.latency or 0) - (r.queue_wait_s or 0) for r in done) / len(done)


def queue_wait_p95_ms(records: Sequence[RequestRecord]) -> float:
    waits = [r.queue_wait_s for r in records if r.ok and r.queue_wait_s is not None]
    return percentiles(waits)["p95"] * 1000 if waits else 0.0


def primary_model(config: GatewayConfig) -> tuple[str, tuple[str, ...]] | None:
    """A model whose route declares a fallback, if the configuration has one."""
    for model, candidates in config.routes.items():
        if len(candidates) > 1:
            return model, candidates
    return None


def default_model(config: GatewayConfig) -> str:
    return next(iter(config.routes), config.providers[0] if config.providers else "fast")


# -- phase 1: contention ---------------------------------------------------


@dataclass(slots=True)
class Contention:
    """What one period of contention showed."""

    checks: list[Check]
    service_s: float | None = None
    output_tokens: float | None = None
    invariants: list[str] = field(default_factory=list)


async def check_contention(harness: Harness, reporter: Reporter,
                           duration_s: float, seed: int) -> Contention:
    """Does a tenant's reserved floor actually protect it from a neighbour?

    Also samples the scheduler's own limits while load is running. Those are
    internal invariants rather than configuration questions -- a user cannot act
    on them -- so they are collapsed into a single line instead of competing
    with the findings that are theirs to fix.
    """
    config = harness.config
    tenants = list(config.tenants)
    protected = max(tenants, key=lambda t: t.reserved_concurrency, default=None)
    if len(tenants) < 2 or protected is None or protected.reserved_concurrency == 0:
        return Contention([Check.skip(
            "a reserved floor protects its tenant",
            "needs two tenants with at least one reserved_concurrency floor",
            "set reserved_concurrency on the tenant whose latency you care about",
        )])

    noisy = next(t for t in tenants if t.id != protected.id)
    model = default_model(config)

    warm = await load_for(harness, protected.id, rate=1.5, duration_s=3.0,
                          model=model, seed=seed)
    service_s = service_seconds(warm.records)
    served = [r for r in warm.records if r.ok and r.output_tokens]
    output_tokens = (
        sum(r.output_tokens for r in served) / len(served) if served else None
    )

    protected_rate = max(0.5, protected.reserved_concurrency / service_s * 0.7)
    reporter.event(
        f"'{noisy.id}' floods while '{protected.id}' offers {protected_rate:.1f} req/s "
        f"-- inside the {protected.reserved_concurrency} slots its floor guarantees"
    )

    observed = Observed()
    sampler = asyncio.create_task(sample(harness, observed, duration_s + 2))
    flood = asyncio.create_task(
        load_for(harness, noisy.id, rate=max(30.0, protected_rate * 20),
                 duration_s=duration_s, model=model, seed=seed + 1)
    )
    protected_out = await load_for(harness, protected.id, protected_rate,
                                   duration_s, model, seed)
    # Stop the flood rather than waiting out its backlog: those requests are
    # meant to be rejected at their deadline, and sitting through deadline_s
    # would make this command far slower than it needs to be.
    flood.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await flood
    await sampler

    ok = [r for r in protected_out.records if r.ok]
    achieved = len(ok) / duration_s
    wait_ms = queue_wait_p95_ms(protected_out.records)
    held = achieved >= 0.8 * protected_rate and wait_ms <= PROTECTED_QUEUE_WAIT_MS

    checks = [Check.result(
        "a reserved floor protects its tenant", held,
        f"'{protected.id}' held {achieved:.2f} of {protected_rate:.2f} req/s, "
        f"queue wait p95 {wait_ms:.0f}ms",
        f"it was squeezed by '{noisy.id}' despite a floor of "
        f"{protected.reserved_concurrency} slots",
        f"that floor sustains about {protected.reserved_concurrency / service_s:.1f} req/s "
        f"at {service_s:.1f}s per request -- raise reserved_concurrency, or expect "
        f"less of this tenant",
    )]

    # Internal invariants, checked but not itemised.
    broken = []
    if observed.max_inflight > config.max_concurrency:
        broken.append(
            f"{observed.max_inflight} in flight exceeded max_concurrency "
            f"{config.max_concurrency}"
        )
    for t in tenants:
        seen = observed.max_per_tenant.get(t.id, 0)
        if t.max_concurrency is not None and seen > t.max_concurrency:
            broken.append(f"{t.id} reached {seen} > ceiling {t.max_concurrency}")
        queued = observed.max_queue_per_tenant.get(t.id, 0)
        if queued > t.max_queue_depth:
            broken.append(f"{t.id} queued {queued} > max_queue_depth {t.max_queue_depth}")

    return Contention(checks, service_s, output_tokens, broken)


async def wait_until_idle(harness: Harness, timeout_s: float = 15.0) -> bool:
    """Wait for in-flight work to finish before reading the ledger.

    Reservations are released as requests end, so reading budgets while requests
    are still unwinding measures the moment rather than the invariant.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    async with httpx.AsyncClient(timeout=5.0) as c:
        while loop.time() < deadline:
            with contextlib.suppress(httpx.HTTPError, ValueError, KeyError):
                stats = await harness.stats(c)
                if stats["inflight"] == 0 and stats["queue_depth"] == 0:
                    return True
            await asyncio.sleep(0.25)
    return False


async def accounting_invariants(harness: Harness) -> list[str]:
    """Budget accounting problems, if any. Internal, so reported as one line."""
    budgeted = [t for t in harness.config.tenants if t.budget_tokens is not None]
    if not budgeted:
        return []
    if not await wait_until_idle(harness):
        return ["gateway did not become idle, so budgets could not be read reliably"]

    async with httpx.AsyncClient(timeout=5.0) as c:
        stats = await harness.stats(c)

    broken = []
    for t in budgeted:
        budget = stats["tenants"].get(t.id, {}).get("budget") or {}
        if budget.get("spent", 0) > (budget.get("limit") or 0):
            broken.append(f"{t.id} spent {budget['spent']} of {budget['limit']}")
        if budget.get("reserved_in_flight", 0) != 0:
            broken.append(f"{t.id} still holds {budget['reserved_in_flight']} reserved")
    return broken


# -- phase 2: provider failure ---------------------------------------------


async def check_failover(harness: Harness, reporter: Reporter,
                         duration_s: float, seed: int) -> list[Check]:
    config = harness.config
    route = primary_model(config)
    if route is None:
        return [
            Check.skip(
                "traffic survives a provider failure",
                "no model has a fallback provider, so no failover is claimed",
                "add a second candidate under [routes], e.g. "
                'fast = ["fast", "backup"]',
            ),
            Check.skip(
                "a failing provider is taken out of rotation",
                "needs a fallback to route to before it can be observed safely",
            ),
        ]

    model, candidates = route
    primary, fallback = candidates[0], candidates[1]
    tenant = config.tenants[0]

    # How many failures the configured breaker needs before it can trip, and
    # whether this run can realistically produce that many.
    needed = max(config.breaker.min_samples,
                 int(config.breaker.window * config.breaker.failure_threshold))
    reporter.detail(
        f"model '{model}' falls back from '{primary}' to '{fallback}'; "
        f"its breaker needs about {needed} failures to trip"
    )
    reporter.event(f"'{primary}' starts failing for every request")

    async with httpx.AsyncClient(timeout=10.0) as c:
        await c.put(f"{harness.stack.fleet.base_url}/control/profiles/{primary}",
                    json={"faults": {"error_rate": 1.0}})
        try:
            # Modest rate: during the outage requests move to the fallback,
            # which is slower, so a high rate would build a backlog that takes
            # longer to drain than the phase itself.
            outcome = await load_for(harness, tenant.id, rate=5.0,
                                     duration_s=duration_s, model=model, seed=seed)
            health = await harness.providers(c)
            metrics = await harness.metrics(c)
        finally:
            await c.put(f"{harness.stack.fleet.base_url}/control/profiles/{primary}",
                        json={"faults": {"error_rate": 0.0}})

    from switchyard.cli.top import parse_metrics, total

    parsed = parse_metrics(metrics)
    failovers = int(total(parsed, "switchyard_failovers_total"))
    skipped = int(total(parsed, "switchyard_provider_skipped_total", provider=primary))
    observed_failures = int(health.get(primary, {}).get("failures", 0))

    served = sum(1 for r in outcome.records if r.ok)
    attempted = len(outcome.records)
    survival = served / attempted * 100 if attempted else 0.0

    checks = [Check.result(
        "traffic survives a provider failure", survival >= 95.0 and failovers > 0,
        f"{survival:.0f}% served while '{primary}' was down, {failovers} failovers",
        f"requests failed even though '{fallback}' was available",
        f"check that '{fallback}' is healthy and that timeouts.ttft_s "
        f"({config.timeouts.ttft_s:g}s) is short enough to fail over before clients give up",
    )]

    if observed_failures >= needed:
        checks.append(Check.result(
            "a failing provider is taken out of rotation", skipped > 0,
            f"{skipped} requests skipped '{primary}' after {observed_failures} failures",
            "the breaker never opened, so every request kept paying to rediscover "
            "the outage",
            "this is a breaker defect rather than a configuration problem",
        ))
    else:
        checks.append(Check.skip(
            "a failing provider is taken out of rotation",
            f"only {observed_failures} failures occurred; this breaker needs about "
            f"{needed} to trip",
            f"breaker.window is {config.breaker.window}, so at production traffic "
            f"this trips quickly. To see it here, lower breaker.window or run "
            f"`switchyard scenario provider-outage`",
        ))
    return checks


# -- phase 3: shutdown -----------------------------------------------------


async def check_drain(harness: Harness, reporter: Reporter, seed: int) -> list[Check]:
    """Drain while work is in flight, then confirm nothing is left behind."""
    tenant = harness.config.tenants[0]
    model = default_model(harness.config)
    reporter.event("draining the gateway while requests are still running")

    load = asyncio.create_task(
        load_for(harness, tenant.id, rate=4.0, duration_s=3.0, model=model, seed=seed)
    )
    await asyncio.sleep(1.5)

    async with httpx.AsyncClient(timeout=60.0) as c:
        drain = (await c.post(f"{harness.stack.gateway.base_url}/v1/admin/drain",
                              headers=harness.admin_headers)).json()
        health = await c.get(f"{harness.stack.gateway.base_url}/health")
        await load
        stats = await harness.stats(c)

    return [
        Check.result(
            "shutdown finishes running work", bool(drain["drained_cleanly"]),
            f"{drain['inflight_at_start']} in flight finished in "
            f"{drain['waited_s']:.1f}s, {drain['queued_rejected']} queued refused",
            f"{drain['inflight_remaining']} request(s) were abandoned at shutdown",
            f"raise gateway.drain_timeout_s (currently "
            f"{harness.config.drain_timeout_s:g}s)",
        ),
        Check.result(
            "load balancers are told to stop sending", health.status_code == 503,
            f"/health returned {health.status_code} while draining",
            "a draining gateway still advertised itself as ready",
            "this is a gateway defect rather than a configuration problem",
        ),
        Check.result(
            "no capacity is left held", stats["inflight"] == 0 and stats["queue_depth"] == 0,
            f"{stats['inflight']} in flight, {stats['queue_depth']} queued at rest",
            "capacity was still held after everything finished",
            "this is a scheduler defect rather than a configuration problem",
        ),
    ]


# -- orchestration ---------------------------------------------------------


async def run(reporter: Reporter, config_path: str = "switchyard.toml",
              contention_s: float = 8.0, failure_s: float = 6.0,
              seed: int = 1) -> ScenarioResult:
    config = load_config(config_path)
    config.validate()

    reporter.heading("configuration check", f"what does {config_path} mean, and does it hold?")

    static = review(config)
    if not config.tenants:
        reporter.section("What this configuration means")
        reporter.lines(render_interpretation(interpret(config), reporter.style))
        result = ScenarioResult("verify", static)
        result.notes.append(
            "No tenants are configured, so nothing could be verified under load. "
            "Add tenants to check isolation, failover and shutdown behaviour."
        )
        return result

    reporter.note(
        "your real providers are not called: traffic goes to the built-in synthetic "
        "fleet, so this costs nothing and cannot disturb production"
    )
    reporter.note(
        "a copy of your configuration runs with test credentials; every limit, "
        "weight, floor, budget and route is taken from your file unchanged"
    )

    derived, tenant_keys, admin_key = derive_config(config)
    checks: list[Check] = []
    invariants: list[str] = []
    contention = Contention([])

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "verify.toml"
        path.write_text(render_toml(derived))
        stack = Stack(gateway_env={"SWITCHYARD_CONFIG": str(path)})
        harness = Harness(stack, derived, tenant_keys, admin_key)
        try:
            await stack.start(run_seed=seed)
            await register_providers(stack, derived.providers)
            reporter.start()
            reporter.section("Checking behaviour under load")

            contention = await check_contention(harness, reporter, contention_s, seed)
            checks.extend(contention.checks)
            invariants.extend(contention.invariants)
            invariants.extend(await accounting_invariants(harness))

            checks.extend(await check_failover(harness, reporter, failure_s, seed))
            checks.extend(await check_drain(harness, reporter, seed))
        finally:
            stack.stop()

    checks.append(Check.result(
        "scheduler invariants held", not invariants,
        "; ".join(invariants) or "capacity, queue and budget accounting stayed within limits",
        "; ".join(invariants),
        "this is a defect in Switchyard rather than in your configuration; "
        "please report it with your config",
    ))

    reporter.section("What this configuration means")
    reporter.lines(render_interpretation(
        interpret(config, contention.service_s, contention.output_tokens), reporter.style
    ))

    return ScenarioResult("verify", static + checks)

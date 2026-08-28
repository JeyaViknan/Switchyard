"""Scenario: the primary provider fails while traffic is flowing.

Providers go down. The question is whether your clients find out.

The scenario runs steady traffic against a primary provider with a fallback
configured, then makes the primary start returning 5xx. What should happen is
that the first few requests fail over invisibly, the circuit breaker notices the
pattern and stops sending traffic to a provider it knows is broken, and clients
keep getting answers throughout. When the provider recovers, the breaker probes
it and puts it back in rotation.

Every state change is called out as it happens, so the breaker transitions are
visible rather than inferred afterwards.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Sequence
from pathlib import Path

import httpx

from switchyard.bench.harness import Stack
from switchyard.bench.loadgen import LoadSpec, RequestRecord, run_load_detailed
from switchyard.core.auth import mint_admin_key, mint_key
from switchyard.scenarios.base import (
    Check,
    Reporter,
    ScenarioResult,
    TenantSpec,
    pct,
    summarise_rejections,
    write_scenario_config,
)

NAME = "provider-outage"
TITLE = "provider outage"
SUBTITLE = "the primary provider starts failing; do clients notice?"

TENANT = "app"
PRIMARY, FALLBACK = "fast", "slow"


def window(records: Sequence[RequestRecord], lo: float, hi: float) -> list[RequestRecord]:
    """Requests *offered* in a window, not completed in it."""
    return [r for r in records if lo <= r.intended_start < hi]


async def watch_providers(stack: Stack, admin_key: str, reporter: Reporter,
                          duration_s: float, seen: dict[str, str],
                          every_s: float = 1.0) -> None:
    """Announce breaker transitions as they happen, and show periodic state."""
    headers = {"authorization": f"Bearer {admin_key}"}
    deadline = reporter.elapsed + duration_s
    last_status = 0.0
    async with httpx.AsyncClient(timeout=5.0) as c:
        while reporter.elapsed < deadline:
            await asyncio.sleep(every_s)
            try:
                health = (await c.get(f"{stack.gateway.base_url}/v1/providers",
                                      headers=headers)).json()
            except (httpx.HTTPError, ValueError):
                continue

            for name, p in health.items():
                state = p["state"]
                if seen.get(name) and seen[name] != state:
                    reporter.event(
                        f"circuit breaker for '{name}': {seen[name]} -> {state}"
                        + (f"  (retries in {p['reopens_in_s']:.0f}s)"
                           if p.get("reopens_in_s") else "")
                    )
                seen[name] = state

            if reporter.elapsed - last_status >= 4.0:
                last_status = reporter.elapsed
                reporter.status("   ".join(
                    f"{name}: {p['state']:<9} {int(p['successes'])} ok / "
                    f"{int(p['failures'])} failed"
                    for name, p in health.items()
                ))


async def run(reporter: Reporter, capacity: int = 8, healthy_s: float = 8.0,
              outage_s: float = 18.0, recovery_s: float = 12.0,
              request_rate: float = 4.0, seed: int = 1,
              policy: str = "drr") -> ScenarioResult:
    tenant = TenantSpec(TENANT, max_queue_depth=256, deadline_s=30.0)
    raw_key, digest = mint_key(TENANT)
    admin_raw, admin_digest = mint_admin_key()
    total_s = healthy_s + outage_s + recovery_s

    reporter.heading(TITLE, SUBTITLE)
    reporter.section("Setup")
    reporter.detail(f"steady traffic at {request_rate:g} req/s through the gateway")
    reporter.detail(f"model 'fast' routes to '{PRIMARY}', falling back to '{FALLBACK}'")
    reporter.detail(
        f"'{PRIMARY}' will start returning 5xx at {healthy_s:g}s "
        f"and recover at {healthy_s + outage_s:g}s"
    )
    reporter.note("no LLM API key needed: both providers are the built-in synthetic fleet")

    with tempfile.TemporaryDirectory() as tmp:
        config_path = Path(tmp) / "scenario.toml"
        write_scenario_config(
            config_path, [tenant], {TENANT: digest}, admin_digest,
            max_concurrency=capacity, policy=policy,
            routes={PRIMARY: (PRIMARY, FALLBACK), FALLBACK: (FALLBACK,)},
            # A rolling window measures the *recent* failure rate, so how fast
            # the breaker trips depends on traffic rate relative to window size:
            # successes from before the outage have to age out first. At this
            # demo's modest request rate a 50-sample window would take longer
            # than the outage itself, so the scenario uses a shorter one. A
            # busier deployment fills the window in seconds and needs no such
            # adjustment.
            ttft_s=3.0, breaker_min_samples=6, breaker_window=16, cooldown_s=5.0,
        )
        stack = Stack(gateway_env={"SWITCHYARD_CONFIG": str(config_path)})
        seen: dict[str, str] = {}
        try:
            await stack.start(run_seed=seed)
            reporter.watch_hint(stack.gateway.base_url, admin_raw)
            reporter.start()
            reporter.section("Running")

            load = run_load_detailed(LoadSpec(
                url=stack.completions_url, rate=request_rate, duration_s=total_s,
                model=PRIMARY, tenants=(TENANT,), seed=seed, max_tokens=256,
                api_key=raw_key, request_timeout_s=60.0,
            ))
            watcher = asyncio.create_task(
                watch_providers(stack, admin_raw, reporter, total_s, seen)
            )

            async def inject() -> None:
                async with httpx.AsyncClient(timeout=5.0) as c:
                    await asyncio.sleep(healthy_s)
                    reporter.event(f"'{PRIMARY}' starts returning 5xx for every request")
                    await c.put(f"{stack.fleet.base_url}/control/profiles/{PRIMARY}",
                                json={"faults": {"error_rate": 1.0}})
                    await asyncio.sleep(outage_s)
                    reporter.event(f"'{PRIMARY}' recovers")
                    await c.put(f"{stack.fleet.base_url}/control/profiles/{PRIMARY}",
                                json={"faults": {"error_rate": 0.0}})

            outcome, _ = await asyncio.gather(load, inject())
            await watcher

            headers = {"authorization": f"Bearer {admin_raw}"}
            async with httpx.AsyncClient(timeout=5.0) as c:
                final_health = (await c.get(f"{stack.gateway.base_url}/v1/providers",
                                            headers=headers)).json()
                final_stats = (await c.get(f"{stack.gateway.base_url}/v1/scheduler/stats",
                                           headers=headers)).json()
                metrics = (await c.get(f"{stack.gateway.base_url}/metrics",
                                       headers=headers)).text
        finally:
            stack.stop()

    from switchyard.cli.top import parse_metrics, total

    parsed = parse_metrics(metrics)
    failovers = int(total(parsed, "switchyard_failovers_total"))
    skipped = int(total(parsed, "switchyard_provider_skipped_total", provider=PRIMARY))

    t0 = min(r.intended_start for r in outcome.records)
    healthy = window(outcome.records, t0, t0 + healthy_s)
    during = window(outcome.records, t0 + healthy_s, t0 + healthy_s + outage_s)
    after = window(outcome.records, t0 + healthy_s + outage_s, float("inf"))

    def served(records: Sequence[RequestRecord]) -> tuple[int, int]:
        return sum(1 for r in records if r.ok), len(records)

    ok_before, n_before = served(healthy)
    ok_during, n_during = served(during)
    ok_after, n_after = served(after)

    reporter.section("What the client experienced")
    reporter.detail(f"{'before the outage':<22}{ok_before}/{n_before} served "
                    f"({pct(ok_before, n_before):.0f}%)")
    reporter.detail(f"{'during the outage':<22}{ok_during}/{n_during} served "
                    f"({pct(ok_during, n_during):.0f}%)")
    reporter.detail(f"{'after recovery':<22}{ok_after}/{n_after} served "
                    f"({pct(ok_after, n_after):.0f}%)")
    reporter.detail(f"{'gateway response':<22}{failovers} transparent failovers, "
                    f"{skipped} requests skipped a provider it knew was down")

    survived = pct(ok_during, n_during) >= 95.0 if n_during else False
    breaker_reacted = final_health[PRIMARY]["failures"] > 0 and (failovers > 0 or skipped > 0)
    stopped_calling = skipped > 0
    recovered = pct(ok_after, n_after) >= 95.0 if n_after else False
    no_leak = final_stats["inflight"] == 0 and final_stats["queue_depth"] == 0
    errors = summarise_rejections(during)

    return ScenarioResult(NAME, [
        Check.result(
            "clients kept getting answers during the outage", survived,
            f"{pct(ok_during, n_during):.0f}% served "
            f"({', '.join(f'{k} {v}' for k, v in errors.items()) or 'no failures'})",
            "requests failed while the primary provider was down",
        ),
        Check.result(
            "traffic moved to the fallback provider", breaker_reacted,
            f"{failovers} failovers, {int(final_health[PRIMARY]['failures'])} "
            f"failures recorded against '{PRIMARY}'",
            "the gateway did not route around the failing provider",
        ),
        Check.result(
            "stopped calling the failing provider", stopped_calling,
            f"{skipped} requests skipped '{PRIMARY}' while its breaker was open",
            "every request kept paying to rediscover the outage",
        ),
        Check.result(
            "service recovered after the provider did", recovered,
            f"{pct(ok_after, n_after):.0f}% served after recovery",
            "the gateway did not resume normal service",
        ),
        Check.result(
            "no capacity leaked", no_leak,
            f"{final_stats['inflight']} in flight, "
            f"{final_stats['queue_depth']} queued at rest",
            "capacity was still held after everything finished",
        ),
    ])

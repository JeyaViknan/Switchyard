"""Scenario: one tenant floods the gateway, another keeps working.

The situation every shared system eventually has. One tenant starts sending far
more than its share -- a runaway job, a retry storm, a customer who just got
popular -- and the question is whether everyone else notices.

The scenario runs a well-behaved tenant on its own first, to establish what
"normal" looks like for it, then starts a flood twenty times larger and shows
the same tenant's numbers again. If the scheduler works, they barely move: the
flood is absorbed by the tenant that caused it, in the form of rejections,
rather than being spread across everybody.

Run it with `--policy fifo` to see the same workload without fair scheduling.
"""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Sequence
from pathlib import Path

import httpx

from switchyard.bench.harness import Stack
from switchyard.bench.loadgen import LoadSpec, RequestRecord, run_load_detailed
from switchyard.bench.stats import percentiles
from switchyard.core.auth import mint_admin_key, mint_key
from switchyard.scenarios.base import (
    Check,
    Reporter,
    ScenarioResult,
    TenantSpec,
    rate,
    summarise_rejections,
    write_scenario_config,
)

NAME = "noisy-neighbour"
TITLE = "noisy neighbour"
SUBTITLE = "one tenant floods the gateway; can another keep working?"

QUIET, NOISY = "quiet", "noisy"


def window(records: Sequence[RequestRecord], lo: float, hi: float) -> list[RequestRecord]:
    """Requests *offered* in a window.

    Partitioning by completion would credit a request issued just before the
    flood to the flood, since it finishes a couple of seconds later.
    """
    return [r for r in records if lo <= r.intended_start < hi]


def queue_wait_p95_ms(records: Sequence[RequestRecord]) -> float:
    waits = [r.queue_wait_s for r in records if r.ok and r.queue_wait_s is not None]
    return percentiles(waits)["p95"] * 1000 if waits else 0.0


async def watch(stack: Stack, admin_key: str, reporter: Reporter,
                duration_s: float, every_s: float = 3.0) -> None:
    """Print live per-tenant state while the scenario runs."""
    headers = {"authorization": f"Bearer {admin_key}"}
    deadline = reporter.elapsed + duration_s
    async with httpx.AsyncClient(timeout=5.0) as c:
        while reporter.elapsed < deadline:
            await asyncio.sleep(every_s)
            try:
                stats = (await c.get(f"{stack.gateway.base_url}/v1/scheduler/stats",
                                     headers=headers)).json()
            except (httpx.HTTPError, ValueError):
                continue
            parts = []
            for name, t in stats["tenants"].items():
                parts.append(f"{name}: {t['inflight']:>2} running {t['queued']:>4} queued")
            reporter.status(
                f"capacity {stats['inflight']:>2}/{stats['max_concurrency']}   "
                + "   ".join(parts)
            )


async def run(reporter: Reporter, policy: str = "drr", capacity: int = 12,
              baseline_s: float = 8.0, flood_s: float = 18.0,
              quiet_rate: float = 1.0, noisy_rate: float = 40.0,
              seed: int = 1) -> ScenarioResult:
    tenants = [
        # Floor sized by Little's law with headroom: the quiet tenant needs
        # `offered rate x seconds per request` slots to keep up, and anything it
        # needs beyond its floor has to come from contended capacity -- where the
        # neighbour's flood shows up in its latency. The run prints the measured
        # service time so the sizing can be checked against reality.
        TenantSpec(QUIET, weight=1.0, reserved_concurrency=6,
                   max_queue_depth=256, deadline_s=30.0),
        TenantSpec(NOISY, weight=1.0, max_queue_depth=64, deadline_s=10.0),
    ]
    keys = {t.id: mint_key(t.id) for t in tenants}
    admin_raw, admin_digest = mint_admin_key()

    reporter.heading(TITLE, SUBTITLE)
    reporter.section("Setup")
    reporter.detail(
        f"gateway capacity {capacity} concurrent requests, "
        f"{'weighted fair scheduling' if policy == 'drr' else 'FIFO (no fairness)'}"
    )
    reporter.detail(
        f"{QUIET:<7} offers {quiet_rate:>4.1f} req/s   "
        f"reserved floor of 6 slots it can always reach"
    )
    reporter.detail(f"{NOISY:<7} offers {noisy_rate:>4.1f} req/s   no floor, so it gets the rest")
    reporter.note(
        "the floor is what bounds latency. Weight divides contended capacity, but an "
        "in-flight request cannot be preempted, so a tenant without enough reserved "
        "slots still waits for one to free."
    )
    reporter.note(
        "size a floor by Little's law -- offered rate x seconds per request. The "
        "measured service time is printed below so the sizing can be checked."
    )
    reporter.note("no LLM API key needed: requests go to the built-in synthetic provider")

    with tempfile.TemporaryDirectory() as tmp:
        config_path = Path(tmp) / "scenario.toml"
        write_scenario_config(
            config_path, tenants, {k: v[1] for k, v in keys.items()}, admin_digest,
            max_concurrency=capacity, policy=policy,
        )
        stack = Stack(gateway_env={"SWITCHYARD_CONFIG": str(config_path)})
        try:
            await stack.start(run_seed=seed)
            reporter.start()
            reporter.section("Running")

            total_s = baseline_s + flood_s
            quiet_load = run_load_detailed(LoadSpec(
                url=stack.completions_url, rate=quiet_rate, duration_s=total_s,
                model="fast", tenants=(QUIET,), seed=seed, max_tokens=256,
                api_key=keys[QUIET][0], request_timeout_s=60.0,
            ))
            watcher = asyncio.create_task(watch(stack, admin_raw, reporter, total_s))

            async def flood() -> object:
                await asyncio.sleep(baseline_s)
                reporter.event(
                    f"{NOISY} tenant starts flooding at {noisy_rate:g} req/s "
                    f"({noisy_rate / quiet_rate:.0f}x the quiet tenant)"
                )
                return await run_load_detailed(LoadSpec(
                    url=stack.completions_url, rate=noisy_rate, duration_s=flood_s,
                    model="fast", tenants=(NOISY,), seed=seed + 1, max_tokens=256,
                    api_key=keys[NOISY][0], request_timeout_s=60.0,
                ))

            quiet_out, noisy_out = await asyncio.gather(quiet_load, flood())
            await watcher
            reporter.event("flood stops")

            async with httpx.AsyncClient(timeout=5.0) as c:
                final = (await c.get(f"{stack.gateway.base_url}/v1/scheduler/stats",
                                     headers={"authorization": f"Bearer {admin_raw}"})).json()
        finally:
            stack.stop()

    # The quiet tenant ran throughout, so its own behaviour before and during
    # the flood is the comparison that matters -- same tenant, same offered
    # load, only the neighbour changed.
    t0 = min(r.intended_start for r in quiet_out.records)
    flood_start_abs = t0 + baseline_s
    before = window(quiet_out.records, t0, flood_start_abs)
    during = window(quiet_out.records, flood_start_abs, float("inf"))

    before_ok = [r for r in before if r.ok]
    during_ok = [r for r in during if r.ok]
    before_rps = rate(len(before_ok), baseline_s)
    during_rps = rate(len(during_ok), flood_s)
    before_wait = queue_wait_p95_ms(before)
    during_wait = queue_wait_p95_ms(during)

    noisy_rejected = summarise_rejections(noisy_out.records)
    quiet_rejected = summarise_rejections(during)
    noisy_ok = sum(1 for r in noisy_out.records if r.ok)

    service_s = (
        sum((r.latency or 0) - (r.queue_wait_s or 0) for r in during_ok) / len(during_ok)
        if during_ok else 0.0
    )
    needed = quiet_rate * service_s

    reporter.section("What happened to the quiet tenant")
    reporter.detail(
        f"{'slots it needed':<22}{needed:>5.1f}   "
        f"({quiet_rate:g} req/s x {service_s:.1f}s per request), floor is 6"
    )
    reporter.detail(
        f"{'before the flood':<22}{before_rps:>5.2f} req/s served   "
        f"queue wait p95 {before_wait:>6.0f}ms"
    )
    reporter.detail(
        f"{'during the flood':<22}{during_rps:>5.2f} req/s served   "
        f"queue wait p95 {during_wait:>6.0f}ms"
    )
    reporter.detail(
        f"{'the noisy tenant':<22}{rate(noisy_ok, flood_s):>5.2f} req/s served   "
        f"{sum(noisy_rejected.values())} rejected"
    )

    # Against what it asked for, not against its own baseline: the baseline
    # window is short, so its rate is noisy and a comparison to it reads oddly.
    kept_serving = during_rps >= 0.8 * quiet_rate
    stayed_responsive = during_wait <= 1000.0
    absorbed_by_noisy = not quiet_rejected and bool(noisy_rejected)
    no_leak = final["inflight"] == 0 and final["queue_depth"] == 0

    result = ScenarioResult(NAME, [
        Check.result(
            "quiet tenant kept being served", kept_serving,
            f"{during_rps:.2f} of {quiet_rate:g} req/s offered",
            "its throughput collapsed once the neighbour arrived",
        ),
        Check.result(
            "quiet tenant was not made to wait", stayed_responsive,
            f"queue wait p95 {during_wait:.0f}ms",
            "it spent a long time queued behind the flood",
        ),
        Check.result(
            "the flood was charged to the tenant causing it", absorbed_by_noisy,
            f"{sum(noisy_rejected.values())} noisy rejected, "
            f"{sum(quiet_rejected.values())} quiet rejected",
            "rejections landed on the wrong tenant",
        ),
        Check.result(
            "no capacity leaked", no_leak,
            f"{final['inflight']} in flight, {final['queue_depth']} queued at rest",
            "capacity was still held after everything finished",
        ),
    ])
    if policy == "fifo":
        result.notes.append(
            "This run used FIFO, which has no notion of tenant. Re-run without "
            "--policy fifo to see the same workload under fair scheduling."
        )
    else:
        result.notes.append("Re-run with --policy fifo to see this without fair scheduling.")
    return result

"""Fairness under sustained backlog: weighted scheduling against the FIFO baseline.

What this measures
------------------
Three tenants share a gateway whose capacity is well below the offered load:

  interactive  weight 1, low rate   -- wants less than its fair share
  noisy        weight 1, high rate  -- the noisy neighbour, permanently backlogged
  premium      weight 3, high rate  -- also backlogged, entitled to 3x noisy's share

Two questions follow, and they are different questions. The first is isolation:
does a tenant asking for less than its share still get served promptly while
someone else floods the gateway? The second is proportionality: when two tenants
both want more than they can have, is what they get split according to weight?

FIFO answers neither. It has no notion of tenant, so a flood pushes everyone
else's requests arbitrarily far back in a single shared queue, and the split
follows offered load rather than entitlement.

The same workload runs under both policies with the same seed, so the arrival
schedule and every provider response are identical between arms. What differs is
only the scheduling decision.

Sustained backlog matters
-------------------------
A finite burst does not exercise fairness: if every tenant's work eventually
drains, they all complete everything regardless of ordering, and totals come out
equal under any policy. Weighting changes who waits, so the backlogged tenants
have to stay backlogged for the whole measurement window.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from switchyard.bench.harness import Stack
from switchyard.bench.loadgen import LoadSpec, RequestRecord, run_load_detailed
from switchyard.bench.stats import percentiles, write_parquet
from switchyard.core.auth import mint_key


@dataclass(frozen=True, slots=True)
class TenantLoad:
    tenant_id: str
    weight: float
    rate: float
    backlogged: bool
    reserved_concurrency: int = 0


DEFAULT_TENANTS = (
    TenantLoad("interactive", weight=1.0, rate=1.0, backlogged=False),
    TenantLoad("noisy", weight=1.0, rate=30.0, backlogged=True),
    TenantLoad("premium", weight=3.0, rate=30.0, backlogged=True),
)


def write_config(
    path: Path, tenants: Sequence[TenantLoad], keys: dict[str, str],
    max_concurrency: int, policy: str, deadline_s: float, queue_depth: int,
) -> None:
    lines = [
        "[gateway]",
        f"max_concurrency = {max_concurrency}",
        f'scheduling_policy = "{policy}"',
        "",
    ]
    for t in tenants:
        digest = keys[f"{t.tenant_id}:digest"]
        lines += [
            "[[tenants]]",
            f'id = "{t.tenant_id}"',
            f'key_sha256 = "{digest}"',
            f"weight = {t.weight}",
            f"reserved_concurrency = {t.reserved_concurrency}",
            f"max_queue_depth = {queue_depth}",
            f"deadline_s = {deadline_s}",
            "",
        ]
    path.write_text("\n".join(lines))


def tenant_report(records: Sequence[RequestRecord], window_s: float) -> dict[str, object]:
    ok = [r for r in records if r.ok and r.in_window]
    issued = [r for r in records if r.in_window]
    latencies = [r.latency for r in ok if r.latency is not None]
    waits = [r.queue_wait_s for r in ok if r.queue_wait_s is not None]
    rejected: dict[str, int] = {}
    for r in issued:
        if not r.ok:
            key = r.error or f"http_{r.status}"
            rejected[key] = rejected.get(key, 0) + 1

    lat = percentiles(latencies)
    wait = percentiles(waits)
    return {
        "issued": len(issued),
        "completed": len(ok),
        "tokens": sum(r.output_tokens for r in ok),
        "throughput_rps": len(ok) / window_s if window_s else 0.0,
        "tokens_per_s": sum(r.output_tokens for r in ok) / window_s if window_s else 0.0,
        "latency_p50_ms": lat["p50"] * 1000,
        "latency_p95_ms": lat["p95"] * 1000,
        "latency_p99_ms": lat["p99"] * 1000,
        "queue_wait_p50_ms": wait["p50"] * 1000,
        "queue_wait_p95_ms": wait["p95"] * 1000,
        "queue_wait_p99_ms": wait["p99"] * 1000,
        "rejected": rejected or {},
    }


def jain_index(values: Sequence[float]) -> float:
    """Jain's fairness index: 1.0 when every value is identical."""
    if not values or all(v == 0 for v in values):
        return float("nan")
    total = sum(values)
    return (total * total) / (len(values) * sum(v * v for v in values))


async def run_arm(
    policy: str, tenants: Sequence[TenantLoad], duration_s: float, warmup_s: float,
    max_concurrency: int, seed: int, max_tokens: int, deadline_s: float,
    queue_depth: int, out_dir: str,
) -> dict[str, object]:
    keys: dict[str, str] = {}
    for t in tenants:
        raw, digest = mint_key(t.tenant_id)
        keys[t.tenant_id] = raw
        keys[f"{t.tenant_id}:digest"] = digest

    with tempfile.TemporaryDirectory() as tmp:
        config_path = Path(tmp) / "switchyard.toml"
        write_config(config_path, tenants, keys, max_concurrency, policy,
                     deadline_s, queue_depth)

        stack = Stack(gateway_env={"SWITCHYARD_CONFIG": str(config_path)})
        try:
            await stack.start(run_seed=seed)

            outcomes = await asyncio.gather(*[
                run_load_detailed(LoadSpec(
                    url=stack.completions_url, rate=t.rate, duration_s=duration_s,
                    model="fast", tenants=(t.tenant_id,), seed=seed,
                    max_tokens=max_tokens, warmup_s=warmup_s, api_key=keys[t.tenant_id],
                    request_timeout_s=deadline_s + 30.0,
                ))
                for t in tenants
            ])
        finally:
            stack.stop()

    window = duration_s - warmup_s
    per_tenant = {}
    for t, outcome in zip(tenants, outcomes, strict=True):
        per_tenant[t.tenant_id] = tenant_report(outcome.records, window)
        write_parquet(outcome.records, f"{out_dir}/fairness_{policy}_{t.tenant_id}.parquet",
                      label=f"fairness_{policy}_{t.tenant_id}")

    # Proportionality is only meaningful among tenants that actually wanted more
    # than they could have. A tenant served everything it asked for is evidence
    # about isolation, not about how contended capacity was divided.
    backlogged = [t for t in tenants if t.backlogged]
    normalised = [
        float(per_tenant[t.tenant_id]["tokens"]) / t.weight for t in backlogged
    ]
    total_tokens = sum(float(r["tokens"]) for r in per_tenant.values())

    return {
        "policy": policy,
        "tenants": per_tenant,
        "token_share": {
            tid: (float(r["tokens"]) / total_tokens if total_tokens else 0.0)
            for tid, r in per_tenant.items()
        },
        "jain_backlogged": jain_index(normalised),
        "total_tokens": total_tokens,
        "window_s": window,
    }


def print_arm(result: dict) -> None:
    print(f"\n--- policy: {result['policy']} "
          f"(window {result['window_s']:g}s, {result['total_tokens']:.0f} tokens) ---")
    header = (f"{'tenant':<13}{'done':>6}{'tok/s':>9}{'share':>8}"
              f"{'qwait p50':>11}{'qwait p95':>11}{'lat p95':>10}{'rejected':>10}")
    print(header)
    for tid, r in result["tenants"].items():
        rejected = sum(r["rejected"].values()) if r["rejected"] else 0
        print(f"{tid:<13}{r['completed']:>6}{r['tokens_per_s']:>9.1f}"
              f"{result['token_share'][tid]:>7.1%}"
              f"{r['queue_wait_p50_ms']:>10.0f}m{r['queue_wait_p95_ms']:>10.0f}m"
              f"{r['latency_p95_ms']:>9.0f}m{rejected:>10}")
    jain = result["jain_backlogged"]
    if jain == jain:
        print(f"{'':<13}weighted fairness (backlogged tenants), Jain index: {jain:.3f}")


async def run(
    duration_s: float, warmup_s: float, max_concurrency: int, seed: int,
    max_tokens: int, deadline_s: float, queue_depth: int,
    out_dir: str, plot_dir: str,
) -> dict[str, dict]:
    tenants = DEFAULT_TENANTS
    print("Fairness under sustained backlog")
    print(f"  gateway max_concurrency = {max_concurrency}")
    for t in tenants:
        role = "backlogged" if t.backlogged else "under its share"
        print(f"  {t.tenant_id:<13} weight {t.weight:g}  offered {t.rate:g}/s  ({role})")

    results = {}
    for policy in ("fifo", "drr"):
        results[policy] = await run_arm(
            policy, tenants, duration_s, warmup_s, max_concurrency, seed,
            max_tokens, deadline_s, queue_depth, out_dir,
        )
        print_arm(results[policy])

    from switchyard.bench.plots import fairness_comparison

    path = fairness_comparison(results, tenants, f"{plot_dir}/fairness.svg")
    print(f"\nfigure: {path}")
    _write_summary(out_dir, results)
    return results


def _write_summary(out_dir: str, results: dict) -> None:
    """Separate from the async runner: blocking IO, once, after the load is done."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    Path(f"{out_dir}/fairness_summary.json").write_text(json.dumps(results, indent=2))


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Fairness under sustained backlog")
    p.add_argument("--duration", type=float, default=40.0)
    p.add_argument("--warmup", type=float, default=5.0)
    p.add_argument("--max-concurrency", type=int, default=16)
    p.add_argument("--max-tokens", type=int, default=512)
    p.add_argument("--deadline", type=float, default=20.0)
    p.add_argument("--queue-depth", type=int, default=200)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out-dir", default="results")
    p.add_argument("--plot-dir", default="plots")
    args = p.parse_args(argv)

    asyncio.run(run(args.duration, args.warmup, args.max_concurrency, args.seed,
                    args.max_tokens, args.deadline, args.queue_depth,
                    args.out_dir, args.plot_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

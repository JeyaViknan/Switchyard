"""Baseline experiment: what does the gateway cost when it does nothing?

This is week one's deliverable and the reference point for everything after it.
The gateway is a passthrough -- no admission control, no queue, no cache -- so
the numbers are the floor: transport and pump cost that any scheduling policy
has to be measured against.

Process isolation
-----------------
The fleet and the gateway run as separate subprocesses; the load generator runs
here. That matters because an earlier version ran all three on one event loop,
where the generator's own CPU work delayed the gateway it was measuring and the
gateway's work delayed the generator's arrivals. Latency attributed to the
system under test was partly the instrument's. Separate processes mean the
generator can only contaminate a run by falling behind, which
`generator_healthy` reports directly.

Measurement window
------------------
Requests scheduled during warmup are issued but excluded from the statistics, so
connection establishment and cold code paths do not land in the reported
percentiles. Both counts are printed.
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

import httpx

from switchyard.bench.harness import Service
from switchyard.bench.loadgen import LoadSpec, run_load_detailed
from switchyard.bench.plots import distribution, latency_vs_offered_load
from switchyard.bench.stats import summarize, write_parquet


def histogram_quantile(buckets: list[tuple[float, float]], q: float) -> float:
    """Quantile from cumulative histogram buckets, interpolating within a bucket.

    The same approach Prometheus uses. Resolution is limited by bucket width, so
    a value is only as precise as the bucket it lands in -- which is why the
    overhead buckets are dense below 10ms.
    """
    if not buckets or buckets[-1][1] == 0:
        return float("nan")
    target = q * buckets[-1][1]
    prev_bound = prev_count = 0.0
    for bound, count in buckets:
        if count >= target:
            if bound == float("inf"):
                return prev_bound
            span = count - prev_count
            frac = (target - prev_count) / span if span else 0.0
            return prev_bound + (bound - prev_bound) * frac
        prev_bound, prev_count = bound, count
    return buckets[-1][0]


async def scrape_timing_decomposition(base_url: str) -> dict[str, float]:
    """Read the gateway's own view of where request time went.

    This is the honest way to state gateway overhead. Comparing a measured TTFT
    against the fleet's configured median cannot work: provider TTFT variance is
    far larger than the gateway's contribution, so the difference is noise.
    Asking the gateway what it spent is a direct measurement instead of a
    subtraction between two noisy numbers.

    Both quantiles and exact means are reported. Histogram quantiles are limited
    by bucket width -- a quantile that lands in the first bucket is reported at
    its midpoint, so a metric whose observations are all exactly zero reads as
    half the first bucket rather than zero. The mean, computed from the metric's
    own sum and count, has no such artifact.
    """
    async with httpx.AsyncClient(timeout=5.0) as c:
        text = (await c.get(f"{base_url}/metrics")).text

    series: dict[str, list[tuple[float, float]]] = {}
    totals: dict[str, float] = {}
    counts: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        value_str = line.rsplit(" ", 1)[-1]
        try:
            value = float(value_str)
        except ValueError:
            continue
        if "_bucket{" in line:
            name = line.split("_bucket{", 1)[0]
            le = line.split('le="', 1)[1].split('"', 1)[0]
            bound = float("inf") if le == "+Inf" else float(le)
            series.setdefault(name, []).append((bound, value))
        elif "_sum" in line:
            name = line.split("_sum", 1)[0]
            totals[name] = totals.get(name, 0.0) + value
        elif "_count" in line:
            name = line.split("_count", 1)[0]
            counts[name] = counts.get(name, 0.0) + value

    out: dict[str, float] = {}
    for metric in ("switchyard_gateway_overhead_seconds", "switchyard_provider_time_seconds",
                   "switchyard_queue_wait_seconds"):
        merged: dict[float, float] = {}
        for bound, value in series.get(metric, []):
            merged[bound] = merged.get(bound, 0.0) + value
        buckets = sorted(merged.items())
        short = metric.replace("switchyard_", "").replace("_seconds", "")
        for q in (0.50, 0.95, 0.99):
            out[f"{short}_p{int(q * 100)}_ms"] = histogram_quantile(buckets, q) * 1000
        count = counts.get(metric, 0.0)
        out[f"{short}_mean_ms"] = (totals.get(metric, 0.0) / count * 1000) if count else 0.0
    return out


async def run(
    rates: Sequence[float], duration_s: float, warmup_s: float, model: str, seed: int,
    max_tokens: int, max_connections: int, out_dir: str, plot_dir: str,
) -> list[dict]:
    fleet = Service("switchyard.synthetic.app:app")
    gateway = Service(
        "switchyard.gateway.app:create_app", factory=True,
        env={"SWITCHYARD_FLEET_URL": fleet.base_url},
    )

    summaries: list[dict] = []
    latency_samples: dict[str, list[float]] = {}
    ttft_samples: dict[str, list[float]] = {}

    try:
        await fleet.wait_healthy()
        await gateway.wait_healthy()
        async with httpx.AsyncClient(timeout=5.0) as c:
            await c.put(f"{fleet.base_url}/control/seed", json={"run_seed": seed})

        url = f"{gateway.base_url}/v1/chat/completions"
        print(f"fleet={fleet.base_url}  gateway={gateway.base_url}  seed={seed}")
        print(f"window: warmup {warmup_s:g}s discarded, measuring {warmup_s:g}-{duration_s:g}s\n")

        for rate in rates:
            spec = LoadSpec(
                url=url, rate=rate, duration_s=duration_s, model=model, seed=seed,
                max_tokens=max_tokens, warmup_s=warmup_s, max_connections=max_connections,
            )
            outcome = await run_load_detailed(spec)
            summary = summarize(outcome.records, outcome=outcome)
            summary["offered_rate"] = rate
            summaries.append(summary)

            measured = [r for r in outcome.records if r.in_window and r.ok]
            latency_samples[f"{rate:g} rps"] = [
                r.latency for r in measured if r.latency is not None
            ]
            ttft_samples[f"{rate:g} rps"] = [r.ttft for r in measured if r.ttft is not None]

            write_parquet(outcome.records, f"{out_dir}/baseline_rate{rate:g}.parquet",
                          label=f"baseline_rate{rate:g}", spec=spec)

            print(
                f"rate={rate:>5g}  n={summary['requests_in_window']:>4}"
                f"/{summary['requests_total']:<4}  "
                f"p50={summary['latency_p50_ms']:>7.1f}ms  "
                f"p99={summary['latency_p99_ms']:>8.1f}ms  "
                f"ttft_p50={summary['ttft_p50_ms']:>6.1f}ms  "
                f"lag={summary['scheduling_lag_ratio']:.3f}  "
                f"peak_conc={summary['peak_concurrency']:>3}  "
                f"{'ok' if summary['generator_healthy'] else 'GENERATOR UNHEALTHY'}"
            )
            if not summary["generator_healthy"]:
                for problem in summary["generator_problems"]:
                    print(f"          ! {problem}")
        decomposition = await scrape_timing_decomposition(gateway.base_url)
        print("\ngateway's own timing decomposition (all rates pooled):")
        for key, value in decomposition.items():
            print(f"  {key:32} {value:8.3f} ms")
    finally:
        gateway.stop()
        fleet.stop()

    paths = [
        latency_vs_offered_load(
            rates=[s["offered_rate"] for s in summaries],
            p50=[s["latency_p50_ms"] / 1000 for s in summaries],
            p95=[s["latency_p95_ms"] / 1000 for s in summaries],
            p99=[s["latency_p99_ms"] / 1000 for s in summaries],
            throughput=[s["throughput_rps"] for s in summaries],
            goodput=[s["goodput_rps"] for s in summaries],
            path=f"{plot_dir}/baseline_latency_vs_load.svg",
        ),
        distribution(ttft_samples, f"{plot_dir}/baseline_ttft_cdf.svg",
                     xlabel="time to first token (ms)",
                     title="Baseline TTFT by offered load"),
        distribution(latency_samples, f"{plot_dir}/baseline_latency_cdf.svg",
                     xlabel="end-to-end latency (ms)",
                     title="Baseline latency by offered load"),
    ]
    print("\nfigures:")
    for path in paths:
        print(f"  {path}")
    return summaries


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Baseline passthrough experiment")
    p.add_argument("--rates", default="2,5,10,20,40")
    p.add_argument("--duration", type=float, default=12.0)
    p.add_argument("--warmup", type=float, default=2.0,
                   help="seconds of arrivals issued but excluded from statistics")
    p.add_argument("--model", default="fast")
    p.add_argument("--max-tokens", type=int, default=256)
    p.add_argument("--max-connections", type=int, default=1000)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out-dir", default="results")
    p.add_argument("--plot-dir", default="plots")
    args = p.parse_args(argv)

    asyncio.run(run(
        [float(x) for x in args.rates.split(",")], args.duration, args.warmup,
        args.model, args.seed, args.max_tokens, args.max_connections,
        args.out_dir, args.plot_dir,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

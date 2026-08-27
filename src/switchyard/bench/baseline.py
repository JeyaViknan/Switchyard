"""Baseline experiment: what does the gateway cost when it does nothing?

This is week one's deliverable and the reference point for everything after it.
The gateway here is a passthrough -- no admission control, no queue, no cache --
so the numbers it produces are the floor: the transport and pump cost that any
scheduling policy has to be measured against.

It runs the fleet and the gateway in-process so the whole experiment is one
command with no container dependency, and so the run is reproducible from a
fixed seed. That does mean the load generator shares an event loop with the
system under test, which is called out in the report: at high offered load the
generator's own scheduling lag is reported alongside the results precisely so a
run contaminated by that is visible rather than silently wrong.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections.abc import Sequence

import uvicorn

from switchyard.bench.loadgen import LoadSpec, run_load
from switchyard.bench.plots import distribution, latency_vs_offered_load
from switchyard.bench.stats import summarize, write_parquet
from switchyard.synthetic.app import FleetState
from switchyard.synthetic.app import create_app as create_fleet
from switchyard.synthetic.profiles import DEFAULT_FLEET


async def _serve(app, port: int) -> tuple[uvicorn.Server, asyncio.Task, int]:
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110 - uvicorn readiness is a bool
        await asyncio.sleep(0.01)
    return server, task, server.servers[0].sockets[0].getsockname()[1]


async def run(rates: Sequence[float], duration_s: float, model: str, seed: int,
              max_tokens: int, out_dir: str, plot_dir: str) -> dict:
    from switchyard.gateway.app import create_app as create_gateway

    fleet_state = FleetState(dict(DEFAULT_FLEET), run_seed=seed)
    fleet_srv, fleet_task, fleet_port = await _serve(create_fleet(fleet_state), 0)
    gw_srv, gw_task, gw_port = await _serve(
        create_gateway(fleet_url=f"http://127.0.0.1:{fleet_port}",
                       providers=tuple(DEFAULT_FLEET)), 0
    )
    url = f"http://127.0.0.1:{gw_port}/v1/chat/completions"

    summaries, latency_samples, ttft_samples = [], {}, {}
    try:
        for rate in rates:
            spec = LoadSpec(url=url, rate=rate, duration_s=duration_s, model=model,
                            seed=seed, max_tokens=max_tokens)
            records = await run_load(spec)
            summary = summarize(records)
            summary["offered_rate"] = rate
            summaries.append(summary)

            ok = [r for r in records if r.ok]
            latency_samples[f"{rate:g} rps"] = [r.latency for r in ok if r.latency is not None]
            ttft_samples[f"{rate:g} rps"] = [r.ttft for r in ok if r.ttft is not None]

            write_parquet(records, f"{out_dir}/baseline_rate{rate:g}.parquet",
                          label=f"baseline_rate{rate:g}", spec=spec)
            flag = "" if summary["generator_kept_up"] else "  <-- GENERATOR LAGGED, run invalid"
            print(
                f"rate={rate:>5g}  ok={summary['completed_ok']:>4}  "
                f"p50={summary['latency_p50_ms']:>7.1f}ms  "
                f"p99={summary['latency_p99_ms']:>8.1f}ms  "
                f"ttft_p50={summary['ttft_p50_ms']:>6.1f}ms  "
                f"lag_p99={summary['scheduling_lag_p99_ms']:>6.2f}ms{flag}"
            )
    finally:
        for srv, task in ((gw_srv, gw_task), (fleet_srv, fleet_task)):
            srv.should_exit = True
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(task, timeout=5)

    lat = latency_vs_offered_load(
        rates=[s["offered_rate"] for s in summaries],
        p50=[s["latency_p50_ms"] / 1000 for s in summaries],
        p95=[s["latency_p95_ms"] / 1000 for s in summaries],
        p99=[s["latency_p99_ms"] / 1000 for s in summaries],
        throughput=[s["throughput_rps"] for s in summaries],
        goodput=[s["goodput_rps"] for s in summaries],
        path=f"{plot_dir}/baseline_latency_vs_load.svg",
    )
    ttft_plot = distribution(ttft_samples, f"{plot_dir}/baseline_ttft_cdf.svg",
                             xlabel="time to first token (ms)",
                             title="Baseline TTFT by offered load")
    lat_plot = distribution(latency_samples, f"{plot_dir}/baseline_latency_cdf.svg",
                            xlabel="end-to-end latency (ms)",
                            title="Baseline latency by offered load")
    print(f"\nfigures: {lat}\n         {ttft_plot}\n         {lat_plot}")
    return {"summaries": summaries}


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Baseline passthrough experiment")
    p.add_argument("--rates", default="2,5,10,20,40")
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--model", default="fast")
    p.add_argument("--max-tokens", type=int, default=256)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out-dir", default="results")
    p.add_argument("--plot-dir", default="plots")
    args = p.parse_args(argv)

    rates = [float(x) for x in args.rates.split(",")]
    asyncio.run(run(rates, args.duration, args.model, args.seed,
                    args.max_tokens, args.out_dir, args.plot_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

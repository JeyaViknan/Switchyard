"""Recovery timeline: what a provider outage looks like from the client's side.

The reliability tests prove the mechanisms work in isolation. This measures what
they are worth under load, by breaking a provider in the middle of a steady
workload and watching what the client experiences.

Two arms, same workload, same seed, same injected outage:

  with failover     -- `fast` may fall back to `slow`
  without failover  -- `fast` is the only candidate

The difference between them is what the reliability layer buys. The second arm
is not a strawman: it is exactly what the gateway did before this week, and it
is still the correct configuration when there is genuinely only one provider.

Three things are worth watching, and they are not the same thing:

- *error rate* -- whether the client got an answer at all.
- *latency* -- with failover, requests move to a slower provider, so surviving
  the outage is not free. Without failover, once the breaker opens, failures
  get faster: the gateway stops paying to rediscover the outage on every
  request. Fast failure is a real improvement over slow failure.
- *breaker state* -- when the gateway stopped sending traffic to the dead
  provider, which is what separates "absorbing an outage" from "retrying into
  one".
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import numpy as np

from switchyard.bench.harness import Stack
from switchyard.bench.loadgen import LoadSpec, RequestRecord, run_load_detailed
from switchyard.core.auth import mint_key

BROKEN_PROVIDER = "fast"
FALLBACK_PROVIDER = "slow"


@dataclass(slots=True)
class Timeline:
    """Breaker state sampled over the run."""

    samples: list[tuple[float, str]] = field(default_factory=list)

    def add(self, t: float, state: str) -> None:
        if not self.samples or self.samples[-1][1] != state:
            self.samples.append((t, state))

    def transitions(self) -> list[tuple[float, str]]:
        return list(self.samples)


def write_config(path: Path, digest: str, with_failover: bool, max_concurrency: int) -> None:
    route = (
        f'{BROKEN_PROVIDER} = ["{BROKEN_PROVIDER}", "{FALLBACK_PROVIDER}"]'
        if with_failover else f'{BROKEN_PROVIDER} = ["{BROKEN_PROVIDER}"]'
    )
    path.write_text(f"""
[gateway]
max_concurrency = {max_concurrency}
scheduling_policy = "drr"

[routes]
{route}

[timeouts]
connect_s = 2.0
ttft_s = 4.0
inter_token_s = 5.0
total_s = 120.0

[breaker]
failure_threshold = 0.5
min_samples = 10
cooldown_s = 5.0
half_open_probes = 2

[[tenants]]
id = "bench"
key_sha256 = "{digest}"
max_queue_depth = 512
deadline_s = 20.0
""")


async def inject_outage(fleet_url: str, start_s: float, end_s: float, t0: float) -> None:
    """Break the provider for a window, then heal it."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        await asyncio.sleep(max(0.0, t0 + start_s - time.perf_counter()))
        await c.put(f"{fleet_url}/control/profiles/{BROKEN_PROVIDER}",
                    json={"faults": {"error_rate": 1.0}})
        await asyncio.sleep(max(0.0, t0 + end_s - time.perf_counter()))
        await c.put(f"{fleet_url}/control/profiles/{BROKEN_PROVIDER}",
                    json={"faults": {"error_rate": 0.0}})


async def watch_breaker(gateway_url: str, t0: float, duration_s: float,
                        timeline: Timeline) -> None:
    async with httpx.AsyncClient(timeout=5.0) as c:
        while time.perf_counter() - t0 < duration_s:
            try:
                health = (await c.get(f"{gateway_url}/v1/providers")).json()
                timeline.add(time.perf_counter() - t0,
                             health[BROKEN_PROVIDER]["state"])
            except (httpx.HTTPError, KeyError):
                pass
            await asyncio.sleep(0.25)


def bucket(records: Sequence[RequestRecord], t0: float, duration_s: float,
           width_s: float = 1.0) -> dict[str, list[float]]:
    """Per-second error rate, latency percentiles and throughput."""
    n = int(duration_s / width_s)
    out: dict[str, list[float]] = {
        "t": [], "error_rate": [], "p50_ms": [], "p99_ms": [], "completed": []
    }
    for i in range(n):
        lo, hi = i * width_s, (i + 1) * width_s
        window = [
            r for r in records
            if r.completed_at is not None and lo <= (r.completed_at - t0) < hi
        ]
        out["t"].append(lo + width_s / 2)
        if not window:
            out["error_rate"].append(float("nan"))
            out["p50_ms"].append(float("nan"))
            out["p99_ms"].append(float("nan"))
            out["completed"].append(0)
            continue
        ok = [r for r in window if r.ok]
        latencies = [r.latency * 1000 for r in window if r.latency is not None]
        out["error_rate"].append(1 - len(ok) / len(window))
        out["p50_ms"].append(float(np.percentile(latencies, 50)) if latencies else float("nan"))
        out["p99_ms"].append(float(np.percentile(latencies, 99)) if latencies else float("nan"))
        out["completed"].append(len(window))
    return out


async def run_arm(with_failover: bool, rate: float, duration_s: float,
                  outage: tuple[float, float], max_concurrency: int,
                  seed: int, max_tokens: int, tmp: Path) -> dict:
    raw, digest = mint_key("bench")
    config_path = tmp / f"switchyard_{'failover' if with_failover else 'single'}.toml"
    write_config(config_path, digest, with_failover, max_concurrency)

    stack = Stack(gateway_env={"SWITCHYARD_CONFIG": str(config_path)})
    timeline = Timeline()
    try:
        await stack.start(run_seed=seed)
        t0 = time.perf_counter()

        load = run_load_detailed(LoadSpec(
            url=stack.completions_url, rate=rate, duration_s=duration_s,
            model=BROKEN_PROVIDER, tenants=("bench",), seed=seed,
            max_tokens=max_tokens, warmup_s=0.0, api_key=raw,
            request_timeout_s=60.0,
        ))
        outcome, _, _ = await asyncio.gather(
            load,
            inject_outage(stack.fleet.base_url, outage[0], outage[1], t0),
            watch_breaker(stack.gateway.base_url, t0, duration_s, timeline),
        )

        async with httpx.AsyncClient(timeout=5.0) as c:
            metrics = (await c.get(f"{stack.gateway.base_url}/metrics")).text
    finally:
        stack.stop()

    records = outcome.records
    during = [r for r in records
              if r.completed_at is not None
              and outage[0] <= (r.completed_at - t0) < outage[1]]
    ok_during = sum(1 for r in during if r.ok)

    return {
        "arm": "with failover" if with_failover else "single provider",
        "series": bucket(records, t0, duration_s),
        "breaker": timeline.transitions(),
        "requests": len(records),
        "completed_ok": sum(1 for r in records if r.ok),
        "during_outage": len(during),
        "during_outage_ok": ok_during,
        "survival_rate": ok_during / len(during) if during else float("nan"),
        "failovers": _counter(metrics, "switchyard_failovers_total"),
        "skipped": _counter(metrics, "switchyard_provider_skipped_total"),
    }


def _counter(metrics: str, name: str) -> float:
    return sum(
        float(line.rsplit(" ", 1)[1])
        for line in metrics.splitlines()
        if line.startswith(name + "{")
    )


def print_arm(result: dict, outage: tuple[float, float]) -> None:
    print(f"\n--- {result['arm']} ---")
    print(f"  requests {result['requests']}, "
          f"completed {result['completed_ok']} "
          f"({result['completed_ok'] / max(result['requests'], 1):.1%})")
    print(f"  during the outage ({outage[0]:g}-{outage[1]:g}s): "
          f"{result['during_outage_ok']}/{result['during_outage']} served "
          f"({result['survival_rate']:.1%})")
    print(f"  failovers {result['failovers']:.0f}, "
          f"requests skipped by an open breaker {result['skipped']:.0f}")
    states = ", ".join(f"{t:.1f}s {s}" for t, s in result["breaker"])
    print(f"  breaker: {states}")


async def run(rate: float, duration_s: float, outage: tuple[float, float],
              max_concurrency: int, seed: int, max_tokens: int,
              out_dir: str, plot_dir: str) -> dict:
    import tempfile

    print("Provider outage recovery")
    print(f"  {rate:g} req/s for {duration_s:g}s, "
          f"'{BROKEN_PROVIDER}' returns 5xx from {outage[0]:g}s to {outage[1]:g}s")

    results = {}
    with tempfile.TemporaryDirectory() as tmp:
        for with_failover in (False, True):
            key = "failover" if with_failover else "single"
            results[key] = await run_arm(
                with_failover, rate, duration_s, outage, max_concurrency,
                seed, max_tokens, Path(tmp),
            )
            print_arm(results[key], outage)

    from switchyard.bench.plots import outage_timeline

    path = outage_timeline(results, outage, f"{plot_dir}/outage.svg")
    print(f"\nfigure: {path}")
    _write_summary(out_dir, results)
    return results


def _write_summary(out_dir: str, results: dict) -> None:
    """Blocking IO, once, after the load is finished."""
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    Path(f"{out_dir}/outage_summary.json").write_text(json.dumps(results, indent=2))


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Provider outage recovery timeline")
    p.add_argument("--rate", type=float, default=8.0)
    p.add_argument("--duration", type=float, default=45.0)
    p.add_argument("--outage-start", type=float, default=12.0)
    p.add_argument("--outage-end", type=float, default=30.0)
    p.add_argument("--max-concurrency", type=int, default=16)
    p.add_argument("--max-tokens", type=int, default=256)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--out-dir", default="results")
    p.add_argument("--plot-dir", default="plots")
    args = p.parse_args(argv)

    asyncio.run(run(args.rate, args.duration, (args.outage_start, args.outage_end),
                    args.max_concurrency, args.seed, args.max_tokens,
                    args.out_dir, args.plot_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

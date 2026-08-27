# Switchyard

An admission controller and fair-share scheduler for LLM inference capacity.

Switchyard is a scheduler that happens to speak LLM, not a proxy that happens to
schedule. The problem it exists to solve: **LLM requests are non-fungible units
of unknown duration and unknown cost.** A request's duration is proportional to
its output length, which is not knowable before the request runs. Existing
gateways model requests as fungible units of known cost, so they are forced to
choose between reserving the declared `max_tokens` ceiling — which idles most of
the real capacity, because a request declaring 4096 tokens typically emits a few
hundred — and reserving nothing, which permits budget overruns.

Switchyard predicts the output-length distribution, reserves at its p95, settles
against the actual, and clamps `max_tokens` when a tenant is near its limit so
the worst case always fits. That converts a probabilistic guarantee into a hard
one, at the cost of truncating responses near budget exhaustion.

## What this is not

Not a production system. There is no HA, no secrets management, no compliance
story, no multi-region, and no operational hardening. It is a measured
demonstration of specific scheduling properties, and the measurements are the
deliverable.

## Status

Week 1 of 4 complete: the measurement rig.

The rig was built before any gateway feature, deliberately. A benchmark harness
written after the system it measures tends to be shaped by the results it finds;
one written first is shaped by the question being asked.

| Component | State |
|---|---|
| Synthetic provider fleet | Done — deterministic, runtime fault injection |
| Open-loop load generator | Done — Poisson arrivals, SSE-aware, TTFT/ITL capture |
| Gateway | Passthrough only — no auth, limits, cache, or scheduler yet |
| Baseline measurement | Done — see `plots/` |
| Admission control + DRR scheduler | Week 2 |
| Capacity accounting, reliability | Week 3 |
| Semantic-cache experiment, report | Week 4 |

## Why the load generator is open-loop

A closed-loop generator — N workers, each sending its next request when the
previous returns — cannot offer more load than the system absorbs. When the
system slows, the generator slows with it, queueing never appears in the
measurement, and reported latency stays flat right up to the point of collapse.
That artifact is coordinated omission, and it makes latency numbers wrong in the
direction that flatters the system.

Switchyard's generator fixes a Poisson arrival schedule up front and fires each
request at its scheduled time regardless of whether earlier ones have finished.
Every latency is measured from the request's *intended* start.

An open-loop generator can itself become the bottleneck, so every run reports
`scheduling_lag` — how late it fired. If that grows, the run is invalid and says
so. That self-check is what makes the rest of the numbers credible.

## Why a synthetic provider fleet

Comparing two scheduling policies is only valid if the provider behaves
identically under both for the same request. Otherwise the measurement is
provider noise, not policy effect. Real APIs cannot give that guarantee, cost
money per run, and rate-limit exactly when throughput is needed.

The fleet draws every per-request decision — time to first token, output length,
whether this request fails and how — from an RNG seeded by
`(run_seed, request_id)`. Replaying a workload under a different scheduler
reproduces the same provider behavior request for request. Faults are driven at
runtime through `/control/*`, so a benchmark can inject a provider outage
mid-run and measure the recovery timeline.

## Baseline

The gateway is currently a passthrough, so the baseline is the latency floor —
transport and pump cost with no scheduling in the path.

```
rate=    2  ok=  11  p50= 2057ms  p99= 3369ms  ttft_p50= 259ms  lag_p99=  1.66ms
rate=    5  ok=  28  p50= 2315ms  p99= 3483ms  ttft_p50= 225ms  lag_p99=  1.32ms
rate=   10  ok=  49  p50= 2354ms  p99= 3440ms  ttft_p50= 231ms  lag_p99= 10.10ms
rate=   20  ok= 116  p50= 2015ms  p99= 3451ms  ttft_p50= 237ms  lag_p99= 19.75ms
```

TTFT p50 of ~230 ms against a configured provider median of 220 ms: the gateway
adds close to nothing. Latency is flat across offered load and throughput tracks
it linearly, which is the expected shape — an unconstrained passthrough has no
concurrency limit, so nothing queues. The knee appears once there is a scheduler
to produce one.

## Running it

```bash
make install
make check          # lint, types, tests
make bench-baseline # regenerates every figure in plots/
```

Or the full stack with dashboards:

```bash
make up             # gateway :8000, fleet :8100, prometheus :9090, grafana :3000
```

## Layout

```
src/switchyard/
  types.py        normalized request and stream-event types; the adapter contract
  adapters/       provider implementations
  gateway/        HTTP surface and the SSE pump
  obs/            metrics
  synthetic/      the provider fleet (an instrument, not part of the gateway)
  bench/          load generator, statistics, figures, experiments
```

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
| Open-loop load generator | Done — Poisson arrivals, SSE-aware, per-token timing persisted |
| Gateway | Passthrough only — no auth, limits, cache, or scheduler yet |
| Baseline measurement | Done — latency floor only, no saturation knee |
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
its own health:

- **Scheduling lag** — how late each request actually fired, judged as a *ratio*
  of median latency rather than a fixed millisecond bar. 20 ms of lag is
  negligible against a 2 s request and disqualifying against a 50 ms one.
- **Connection saturation** — if concurrency reaches the client's connection
  cap, arrivals block on the pool and the run has silently stopped being
  open-loop.
- **File-descriptor headroom** — checked before the run starts, so fd exhaustion
  cannot masquerade as the system under test refusing connections.

The fleet, the gateway, and the generator run in **separate processes**. An
earlier version ran all three on one event loop, where the generator's own CPU
work delayed the gateway it was measuring; scheduling lag reached 20 ms at only
20 rps. With process isolation the same measurement runs at 0.0–2.5% of median
latency through 40 rps.

Requests scheduled during warmup are issued but excluded from the statistics, so
connection setup and cold code paths do not land in the reported percentiles.

## Why a synthetic provider fleet

Comparing two scheduling policies is only valid if the provider behaves
identically under both for the same request. Otherwise the measurement is
provider noise, not policy effect. Real APIs cannot give that guarantee, cost
money per run, and rate-limit exactly when throughput is needed.

The fleet draws every per-request decision — time to first token, output length,
whether this request fails and how — from an RNG seeded by
`(run_seed, request_id)`, and the load generator derives request ids from
`(seed, index)` rather than randomly. Both halves are required: with random ids
the fleet is deterministic but the workload is not, and two runs of one spec
produce different output lengths. `test_same_seed_reproduces_the_same_workload`
asserts that two runs return identical per-request output tokens.

Faults are driven at runtime through `/control/*`, so a benchmark can inject a
provider outage mid-run. Enabling a fault does not perturb the other draws, so a
faulted run remains comparable to a clean one; that property is also tested.

## Baseline

The gateway is currently a passthrough, so this measures the latency floor:
transport and pump cost with no scheduling in the path. Single run per rate,
seed 1, 12 s each with the first 2 s discarded as warmup.

```
rate=    2  n=  20/22    p50= 2384.9ms  p99= 3298.9ms  ttft_p50= 239.8ms  lag=0.001  peak_conc=  9
rate=    5  n=  42/49    p50= 2318.4ms  p99= 3355.3ms  ttft_p50= 231.4ms  lag=0.025  peak_conc= 15
rate=   10  n=  96/116   p50= 1993.4ms  p99= 3354.4ms  ttft_p50= 218.4ms  lag=0.001  peak_conc= 29
rate=   20  n= 209/239   p50= 2074.3ms  p99= 3349.2ms  ttft_p50= 219.2ms  lag=0.000  peak_conc= 51
rate=   40  n= 416/486   p50= 2173.8ms  p99= 3403.0ms  ttft_p50= 220.3ms  lag=0.007  peak_conc=105
```

`n` is requests in the measurement window over requests issued; `lag` is p99
scheduling lag as a fraction of median latency.

### How much of that is the gateway?

Measured directly from the gateway's own timing decomposition, pooled across all
rates above:

```
gateway_overhead   p50   0.88ms   p95   2.41ms   p99   4.72ms   mean   1.07ms
provider_time      p50 2123ms     p95 3812ms     p99 3962ms     mean 2169ms
queue_wait         mean 0.000ms   (structurally zero: no admission control yet)
```

So the gateway accounts for roughly **0.05% of median request time** at these
settings. This is a direct measurement, not an inference. An earlier version of
this README compared measured TTFT against the fleet's *configured* TTFT median
and concluded the gateway "adds close to nothing" — that method does not work.
Provider TTFT variance (lognormal, σ=0.4) is far larger than the gateway's
contribution, and running the comparison properly returned **−14 ms**, which is
impossible and simply means the signal was below the noise floor. Asking the
gateway what it spent replaces a subtraction of two noisy numbers with one
measurement.

### What this baseline does not establish

- **No saturation point.** Latency is flat and throughput linear across 2–40 rps
  because nothing in the passthrough constrains concurrency — peak concurrency
  only reached 105. Flatness here is a property of the load range, not a
  finding. There is no knee because there is nothing yet to produce one.
- **Single run per rate.** No repeats, so no confidence interval. The plan calls
  for median-of-5; that arrives with the overload experiment.
- **Short window.** 10 s of measured arrivals per rate. Enough for a floor,
  not enough for tail behavior at p99.9.
- **One machine, unpinned.** No CPU isolation; other load on the host will show
  up in the numbers.

The overload experiment will need much higher offered load, repeats, and a knee.
None of that is claimed here.

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

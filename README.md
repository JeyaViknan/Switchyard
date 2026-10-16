<div align="center">

# Switchyard

**An LLM gateway you can break on purpose.**

Multi-tenant scheduling for scarce inference capacity — token-fair queueing,
hard budgets, and failover that ships with the harness that proves it.

[![CI](https://github.com/JeyaViknan/Switchyard/actions/workflows/ci.yml/badge.svg)](https://github.com/JeyaViknan/Switchyard/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-356%20passing-brightgreen.svg)](#testing)
[![Status: beta](https://img.shields.io/badge/status-beta-yellow.svg)](#project-status)

[Quickstart](#quickstart) · [Why](#why-you-would-want-this) · [Results](#measured-results) · [Install](#install) · [Configure](#configure) · [Status](#project-status) · [Design notes](docs/decisions.md)

</div>

---

```bash
git clone https://github.com/JeyaViknan/Switchyard && cd Switchyard
make install
make scenario noisy-neighbour
```

Sixty seconds, no API key, no spend. One tenant floods the gateway at 25× the
rate of its neighbour, and you watch the neighbour not care.

---

## Why you would want this

Put several teams, products or customers behind one pool of LLM capacity and
two things break that ordinary gateways do not fix.

**Capacity is not fungible.** A request holds a provider slot for as long as it
takes to generate, and how long that is depends on output length — which the
caller does not declare and the model does not promise. Limiting by
requests-per-second allocates the wrong unit entirely. One tenant sending
4,000-token completions consumes roughly ten times the resource of one sending
400-token completions at the same rate, and is charged the same.

**Failure is not uniform.** A provider that is slow, a provider returning 5xx,
and a provider that answered and then went silent mid-stream need three
different responses. A single timeout treats them identically and makes all
three wait its full duration.

Switchyard schedules in **tokens**, enforces budgets as a **hard bound**, and
decomposes failure so each kind gets the handling it needs.

### Who it is for

| You have | Switchyard gives you |
|---|---|
| **A self-hosted vLLM/Ollama box shared by several teams** | The case it is built for. Capacity is genuinely fixed, so fair-share scheduling decides who waits. |
| **One API account fronting several products or customers** | Per-tenant floors, ceilings, weights and token budgets that cannot be overrun. |
| **An internal platform billing teams for LLM spend** | Spend metered per tenant, clamped at the limit, and durable across restarts. |
| **A gateway you need to justify to someone** | `switchyard verify` runs your actual config under adversarial load and reports what it measured. |

### Who it is not for

Worth saying plainly, because the wrong adoption helps nobody:

- **You call one hosted API as one team.** Capacity is elastic; buy more. You
  do not have a scheduling problem.
- **You need 100 provider integrations.** Use [LiteLLM](https://github.com/BerriAI/litellm).
  Switchyard speaks one wire format — OpenAI-compatible — on purpose.
- **You need HA today.** Switchyard is a single process. A crash is an outage.

### How it compares

| | Switchyard | Typical LLM gateway |
|---|---|---|
| Fairness unit | **Tokens**, settled against actual consumption | Requests per minute |
| Capacity guarantee | **Reserved floors** per tenant + shared pool | Global limit, first-come-first-served |
| Budget enforcement | **Hard** — reservation is the ceiling, `max_tokens` clamped | Advisory; overrun is detected after the fact |
| Mid-stream failure | Typed terminal error with tokens delivered | Stream just stops |
| Timeouts | Four: connect / first-token / inter-token / total | One |
| Provider coverage | Any OpenAI-compatible endpoint | Dozens of bespoke adapters |
| Can you test its claims? | **`switchyard verify`, against your own config** | Read the docs and hope |

---

## Quickstart

```bash
make install
```

```bash
make dev
```

Gateway on `:8000`, synthetic provider fleet on `:8100`. Then:

```bash
curl -sN localhost:8000/v1/chat/completions -H "Authorization: Bearer sk_sy_acme_b6525e6c60405226fc726bdda422dd2f" -H 'Content-Type: application/json' -d '{"model":"fast","messages":[{"role":"user","content":"hello"}],"stream":true}'
```

The keys in [`switchyard.toml`](switchyard.toml) are development keys, committed
so a fresh checkout works immediately. They are minted against a built-in
development pepper and are worthless anywhere that sets `SWITCHYARD_KEY_PEPPER`.
Mint real ones with `switchyard keys mint <tenant>`.

### Watch it work

```bash
switchyard top --key sk_sy_admin_672e27a104b9ede9182f129f4b288395
```

```
switchyard http://127.0.0.1:8000   policy drr   capacity ########.... 16/24   queued 20

  TENANT         WEIGHT  FLOOR  INFLIGHT  QUEUED    TOKENS            BUDGET
  acme              3.0      6        16      20      34.2K  1.9M / 2.0M left
  globex            1.0      4         0       0       8.1K  492K / 500K left

  PROVIDER      STATE             OK  FAILED     TTFT   NOTE
  fast          closed          1204      16    231ms
  slow          open              88     104        -   retries in 3.2s
```

---

## Break it on purpose

The synthetic provider fleet and the load generator are product features, not
test scaffolding. You can reproduce every number below on your laptop.

### A tenant floods the gateway

```bash
make scenario noisy-neighbour
```

One tenant runs alone to establish its normal, then a neighbour starts sending
twenty-five times more.

```
    8.1s  >> noisy tenant starts flooding at 25 req/s (25x the quiet tenant)
   15.1s     capacity 11/12   quiet:  5 running   0 queued   noisy:  6 running  64 queued

  What happened to the quiet tenant
    slots it needed         2.4   (1 req/s x 2.4s per request), floor is 6
    before the flood       0.75 req/s served   queue wait p95      0ms
    during the flood       0.94 req/s served   queue wait p95      0ms
    the noisy tenant        479 tokens/s served   401 rejected

    PASS  quiet tenant kept being served                  0.94 of 1 req/s offered
    PASS  quiet tenant was not made to wait               queue wait p95 0ms
    PASS  the flood was charged to the tenant causing it  401 noisy rejected, 0 quiet rejected
```

The floor is what bounds latency, and it has to be sized: a tenant needs
`offered rate × seconds per request` slots, and anything beyond that comes from
contended capacity where the neighbour's flood shows up in its latency. The
scenario prints that arithmetic so the sizing can be checked rather than assumed.

Run it with `--policy fifo` to watch the same workload without fair scheduling.

### A provider dies mid-traffic

```bash
make scenario provider-outage
```

```
    8.0s  >> 'fast' starts returning 5xx for every request
   12.1s  >> circuit breaker for 'fast': closed -> open  (retries in 4s)
   26.0s  >> 'fast' recovers

  What the client experienced
    during the outage     72/72 served (100%)
    after recovery        42/43 served (98%)
    gateway response      10 transparent failovers, 40 requests skipped a
                          provider it knew was down

    PASS  clients kept getting answers during the outage  100% served
    PASS  stopped calling the failing provider            40 requests skipped 'fast'
    PASS  service recovered after the provider did        98% served after recovery
```

**Failover is phased on what has already been delivered.** Before the first
token, another provider serves the request and the client never knows. After it,
Switchyard will not switch — splicing a second continuation onto a partial
answer would return corrupted output under a success status. Instead the stream
ends with a typed error carrying exactly how many tokens arrived.

---

## Measured results

Every figure is regenerated from scratch by `make bench`. If a plot cannot be
reproduced by one command, it is not evidence.

### Fair scheduling protects a tenant from its neighbours

<img src="plots/fairness.svg" alt="Token share and latency under contention, FIFO versus weighted fair queueing" width="100%">

Three tenants, one of them flooding. Same workload, same capacity, two policies:

| Interactive tenant | FIFO | Token-fair (DRR) | |
|---|---|---|---|
| Queue wait p95 | 19,980 ms | **273 ms** | **73× lower** |
| Latency p95 | 24,449 ms | **5,522 ms** | **4.4× lower** |
| Token share | 9.2% | **13.5%** | closer to its weight |

Under FIFO the interactive tenant waits behind the flood and gets 9% of
capacity. Under weighted fair queueing it is served almost immediately.

### Failover survives a total provider outage

<img src="plots/outage.svg" alt="Client-visible error rate through a provider outage, with and without failover" width="100%">

A provider returns 5xx for every request for 18 seconds, under live traffic:

| | Single provider | With failover |
|---|---|---|
| Served during the outage | 16 / 120 — **13%** | 48 / 48 — **100%** |
| Peak client-visible error rate | **100%** | **0%** |
| Transparent failovers | 0 | 26 |
| Requests that skipped the dead provider | 176 | 34 |

The breaker opens, traffic routes to the standby, and clients see nothing. When
the provider recovers, a bounded half-open probe lets it back in.

### Baseline behaviour

[Latency vs. offered load](plots/baseline_latency_vs_load.svg) ·
[Latency CDF](plots/baseline_latency_cdf.svg) ·
[Time-to-first-token CDF](plots/baseline_ttft_cdf.svg)

Load is generated **open-loop** — Poisson arrivals, not a fixed worker pool — so
the latency numbers are free of coordinated omission. The generator records
intended start against actual start to prove it.

---

## Install

### From source

Works today, and is what the scenarios and benchmarks expect:

```bash
git clone https://github.com/JeyaViknan/Switchyard && cd Switchyard && make install
```

### Docker Compose

Gateway, synthetic fleet, Prometheus and a provisioned Grafana dashboard:

```bash
make up
```

Gateway on `:8000`, Grafana on `:3000`. Tenant spend lives on a named volume, so
it survives restarts. `make down` tears it all down including the volume.

### From PyPI

Published as `switchyard-gateway` (the name `switchyard` was already taken; the
installed command is still `switchyard`), from the first tagged release onward:

```bash
pip install switchyard-gateway
```

### Container image

```bash
docker pull ghcr.io/jeyaviknan/switchyard:latest
```

Both of the above are wired up in
[`.github/workflows/release.yml`](.github/workflows/release.yml) and publish on
a `v*` tag. See [Project status](#project-status) for what has actually shipped.

---

## Configure

A minimal two-tenant gateway:

```toml
[gateway]
max_concurrency = 24          # what the upstream can absorb, not what you can hold open
scheduling_policy = "drr"     # "drr" for weighted fair sharing, "fifo" for the baseline
state_file = "/var/lib/switchyard/state.json"   # keeps spend across restarts

[routes]
fast = ["openai", "local"]    # fall back to the local model when OpenAI is down

[providers.openai]
base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"     # read from the environment, never stored here
upstream_model = "gpt-4o-mini"

[providers.local]
base_url = "http://localhost:11434/v1"   # Ollama, vLLM, anything OpenAI-compatible

[[tenants]]
id = "acme"
key_sha256 = "..."            # switchyard keys mint acme
weight = 3.0
reserved_concurrency = 6      # capacity nobody else can take
max_concurrency = 18          # ceiling, so it cannot take everything
budget_tokens = 2_000_000
```

Providers with no `[providers.<name>]` section resolve to the built-in synthetic
fleet, which is why a fresh checkout needs no credentials at all.

### Sizing the floors

A tenant needs `offered_rate × seconds_per_request` concurrent slots to be
served without queueing — Little's law. Below that it waits; above it, the extra
comes from contended capacity. `switchyard verify` measures the seconds per
request for you and prints the arithmetic.

### Budgets survive restarts

Set `gateway.state_file` and per-tenant spend is written atomically and reloaded
at startup. Without it, a restart hands every tenant a fresh allowance — which
`switchyard check` will tell you about rather than leaving you to discover.

A crash can lose at most `state_flush_interval_s` of recorded spend (default 2s),
so a tenant is briefly under-charged, never over-charged. Writing on every
request is deliberately not offered: an `fsync` per completion would make a
gateway whose whole job is throughput wait on a disk.

---

## Does *your* configuration hold?

```bash
switchyard check
```

Static, instant, starts nothing:

```
    PASS  capacity is fully allocatable        12 reserved of 24, 12 shared
    PASS  tenants are isolated from each other 3 of 3 tenants have a reserved floor
    PASS  no tenant can take the whole gateway 3 of 3 have a ceiling
    PASS  operational endpoints are protected  admin key configured
    SKIP  budgets survive a restart            no gateway.state_file set: 3 budgeted
                                               tenant(s) start again from zero spend
          -> set gateway.state_file to a writable path to keep spend across restarts

  What this configuration means
    capacity          24 concurrent requests: 12 reserved as floors, 12 shared
    tenant 'acme'     floor 6, ceiling 18, 60% of contended capacity
    model 'slow'      only slow
                      no fallback: a failure here reaches the client
    circuit breaker   opens after about 25 failures within the last 50
```

```bash
switchyard verify
```

~35s. Runs your configuration — every limit, weight, floor, budget and route —
against the synthetic fleet, then reports what it measured:

```
    tenant 'acme'     floor 6, ceiling 18, 60% of contended capacity
                      -- its floor alone sustains about 3.4 req/s
                      at the 1.8s per request measured here
      budget          2,000,000 tokens is about 15,625 more requests

    PASS  a reserved floor protects its tenant     'acme' held 2.38 of 2.36 req/s
    PASS  traffic survives a provider failure      100% served, 25 failovers
    PASS  shutdown finishes running work           2 in flight finished in 0.8s
```

It never calls your real providers and exits non-zero on failure, so it can gate
a deploy. A guarantee your configuration does not claim is reported as `SKIP`
with what to add, not silently passed.

---

## API

OpenAI-compatible, so existing SDKs work by changing the base URL.

| Endpoint | Auth | Purpose |
|---|---|---|
| `POST /v1/chat/completions` | tenant | Streaming and non-streaming completions |
| `GET /health` | none | Liveness |
| `GET /metrics` | admin | Prometheus exposition |
| `GET /v1/scheduler/stats` | admin | Live queues, capacity, per-tenant budgets |
| `GET /v1/providers` | admin | Provider health and breaker state |
| `POST /v1/admin/drain` | admin | Refuse queued work, let running work finish |

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="sk_sy_acme_...")
stream = client.chat.completions.create(
    model="fast",
    messages=[{"role": "user", "content": "hello"}],
    stream=True,
)
```

Admin endpoints take a separate credential from tenant keys: they expose every
tenant's usage and can take the gateway out of service, so no single tenant
should hold them.

---

## Observability

`make up` brings up Prometheus and Grafana with an 11-panel dashboard already
provisioned — capacity in use, per-tenant queue depth and token share, breaker
state, TTFT and inter-token latency, budget headroom, admission rejections by
reason.

Structured logs in text or JSON (`SWITCHYARD_LOG_FORMAT=json`). The logging
layer **refuses** a known set of field names, so logging a prompt is a loud
failure rather than a silent leak.

---

## Testing

```bash
make check      # ruff, mypy, 356 tests -- about a minute
make test-all   # adds the scenario end-to-end tests, which run real load
make bench      # regenerate every figure in plots/
```

mypy runs with `disallow_untyped_defs` on `switchyard.core` and
`switchyard.adapters` — the scheduler, budgets and routing, where a type error
costs real money. Benchmark tooling and the synthetic fleet are instruments, and
are not held to it.

---

## Project status

**Beta.** Complete and tested for its intended shape: a single-replica,
self-hosted gateway. Not yet proven in production by anyone, including its
author — see [what this is not](#what-this-is-not).

### Completeness

| Subsystem | Status | Evidence |
|---|---|---|
| Token-fair scheduling, floors, ceilings | **Complete** | `noisy-neighbour`, fairness benchmark, 73× queue-wait improvement |
| Admission control, bounded queues, deadlines | **Complete** | Unit + property tests, scheduler stats |
| Hard token budgets, clamping, settlement | **Complete** | `test_budget.py`, clamp path exercised end to end |
| Durable spend across restarts | **Complete** | `test_persistence.py`, restart test |
| Timeout decomposition | **Complete** | `test_timeouts.py`, four distinct error classes |
| Circuit breaking + phased failover | **Complete** | `provider-outage`, 100% survival through a total outage |
| Graceful drain | **Complete** | Drain test, `verify` check |
| OpenAI-compatible providers | **Complete** | `test_providers.py` against a key-enforcing stub |
| Verification harness (`check` / `verify`) | **Complete** | 11 checks, exits non-zero |
| CLI, `top`, scenarios | **Complete** | `test_cli.py`, `test_scenarios.py` |
| Metrics, dashboard, structured logging | **Complete** | Provisioned Grafana, redaction guard |
| Docker Compose deployment | **Complete** | Healthchecks, state volume |
| **Live traffic against a paid provider** | **Not done** | Path is tested; no real spend has run through it |
| **High availability / multi-replica** | **Not planned for 1.0** | In-process scheduler is a deliberate choice |
| PyPI + GHCR publication | **Automated, not yet tagged** | `release.yml` publishes on `v*` |

### Milestones

- [x] **M1 — Measurement rig.** Synthetic fleet, open-loop load generator,
      reproducible figures. *Without this first, nothing after it is credible.*
- [x] **M2 — The scheduler.** Weighted fair queueing in tokens, floors,
      ceilings, admission control, budgets.
- [x] **M3 — Reliability.** Timeout decomposition, circuit breaking, phased
      failover, cancellation, graceful drain.
- [x] **M4 — Product surface.** CLI, scenarios, `check`/`verify`, dashboard,
      deployment, real providers.
- [x] **M5 — Durability.** Spend that survives a restart.
- [ ] **M6 — First tagged release.** `v0.1.0` to PyPI and GHCR.
- [ ] **M7 — A real deployment.** Live traffic through a paid provider, and the
      numbers from it published here.
- [ ] **M8 — Budget windows.** Monthly refill rather than lifetime totals.

### Roadmap, and what is deliberately excluded

Planned: budget refill windows · per-tenant model allowlists · a prefix-aware
routing policy for self-hosted vLLM, where routing to the replica that already
holds the KV prefix is the single largest available win.

Excluded on purpose, with reasons in [`docs/decisions.md`](docs/decisions.md):
Kubernetes manifests · a message broker · a distributed queue · a plugin
framework · a web admin UI · dozens of provider adapters. Each would add
surface area without adding a guarantee.

---

## What this is not

Not a production system, and saying so is cheaper than having you find out.

- **No HA.** Single process. A crash is an outage; spend survives it if
  `state_file` is set, in-flight requests do not.
- **No TLS, no secrets manager.** Run it behind a terminating proxy; provider
  keys come from the environment.
- **Budgets are lifetime totals.** No monthly reset yet (M8).
- **No one has run it in production**, including me. The guarantees are
  demonstrated under synthetic load, which is a real but different claim.

See [SECURITY.md](SECURITY.md) for the full boundary.

---

## Documentation

| | |
|---|---|
| [`docs/decisions.md`](docs/decisions.md) | Why the system is built this way — including the bugs found along the way and what they changed |
| [`docs/demo.md`](docs/demo.md) | A recorded end-to-end run |
| [`CHANGELOG.md`](CHANGELOG.md) | What changed, when |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | How to work on it |
| [`SECURITY.md`](SECURITY.md) | What is protected, and what is not |

### Layout

```
src/switchyard/
  core/        config, auth, scheduler, queue policies, prediction, budgets,
               health, routing, persistence
  gateway/     HTTP surface and the SSE pump
  adapters/    OpenAI-compatible provider client
  obs/         metrics and structured logging
  synthetic/   the provider fleet
  scenarios/   runnable demonstrations
  bench/       load generator and experiments
  verify.py    configuration verification
```

---

## Contributing

Issues and pull requests welcome — read [CONTRIBUTING.md](CONTRIBUTING.md)
first, particularly the part about what the project optimises for. The short
version: every component should enforce an invariant or produce a number.

## License

[MIT](LICENSE).

# Switchyard

**An LLM gateway you can break on purpose.**

Switchyard is a self-hostable multi-tenant gateway that schedules scarce
inference capacity fairly, enforces token budgets, and keeps serving when
providers fail. It ships with the harness that proves it — break a provider
mid-traffic, flood one tenant, and watch what happens, with no LLM API key and
no spend.

```bash
git clone https://github.com/JeyaViknan/Switchyard && cd Switchyard
make install
make scenario noisy-neighbour
```

Or the whole story in one command: `make demo`. A recorded run is in
[docs/demo.md](docs/demo.md).

---

## The problem

Put several tenants behind one set of LLM providers and two things go wrong.

**Capacity is not fungible.** A request holds a provider slot for as long as it
takes to generate, and how long that is depends on output length — which the
caller does not declare and the model does not promise. Rate limiting by
requests per second allocates the wrong thing.

**Failure is not uniform.** A provider that is slow, a provider returning 5xx,
and a provider that answered and then stalled all need different responses. One
timeout treats them identically and makes all three wait its full duration.

## What Switchyard does

- **Weighted fair scheduling** across tenants, measured in **tokens**, with
  reserved floors that guarantee capacity and ceilings that cap it.
- **Admission control** with bounded queues and deadlines, so overload sheds
  load instead of accumulating work nobody is waiting for.
- **Hard token budgets** — reserve, settle, and clamp `max_tokens` so spend
  cannot exceed a limit.
- **Timeout decomposition** into connect / first-token / inter-token / total,
  each producing a distinct, differently-handled failure.
- **Phased failover** and circuit breaking that route around a failing provider
  without ever splicing two answers together.
- **Graceful drain** that refuses queued work and lets running work finish.

## Why it is different

Most gateways can be configured. Switchyard can be **interrogated**.

The synthetic provider fleet and load generator are product features, not test
scaffolding. That means you can watch the scheduler protect a tenant, watch a
breaker open, and ask whether *your* configuration actually delivers what it
promises — in under a minute, with nothing to sign up for.

---

## Quickstart

```bash
make install
make dev          # gateway :8000, synthetic providers :8100
```

```bash
curl -sN localhost:8000/v1/chat/completions \
  -H "Authorization: Bearer sk_sy_acme_b6525e6c60405226fc726bdda422dd2f" \
  -H 'Content-Type: application/json' \
  -d '{"model":"fast","messages":[{"role":"user","content":"hello"}],"stream":true}'
```

The keys in `switchyard.toml` are development keys, committed so a fresh
checkout works immediately. They are minted against a built-in development
pepper and are worthless anywhere that sets `SWITCHYARD_KEY_PEPPER`. Mint real
ones with `switchyard keys mint <tenant>`.

Watch it live, from another terminal:

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

### A tenant floods the gateway

```bash
make scenario noisy-neighbour
```

One tenant runs alone to establish its normal, then a neighbour starts sending
twenty-five times more. The protected tenant's numbers barely move.

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
`offered rate x seconds per request` slots, and anything beyond that comes from
contended capacity where the neighbour's flood shows up in its latency. The
scenario prints that arithmetic so the sizing can be checked rather than assumed.

Run it with `--policy fifo` to see the same workload without fair scheduling.

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

## Does *your* configuration hold?

```bash
switchyard check     # instant: is it valid, and what does it mean?
switchyard verify    # ~35s: does it behave that way under load?
```

`check` is static and starts nothing:

```
    PASS  capacity is fully allocatable        12 reserved of 24, 12 shared
    PASS  tenants are isolated from each other 3 of 3 tenants have a reserved floor
    PASS  operational endpoints are protected  admin key configured

  What this configuration means
    capacity          24 concurrent requests: 12 reserved as floors, 12 shared
    tenant 'acme'     floor 6, ceiling 18, 60% of contended capacity
    model 'slow'      only slow
                      no fallback: a failure here reaches the client
    circuit breaker   opens after about 25 failures within the last 50
```

`verify` runs your configuration — every limit, weight, floor, budget and route
— against the synthetic fleet, then reports what it measured:

```
    tenant 'acme'     floor 6, ceiling 18, 60% of contended capacity
                      -- its floor alone sustains about 3.4 req/s
                      at the 1.8s per request measured here
      budget          2,000,000 tokens is about 15,625 more requests

    PASS  a reserved floor protects its tenant     'acme' held 2.38 of 2.36 req/s
    PASS  traffic survives a provider failure      100% served, 25 failovers
    PASS  shutdown finishes running work           2 in flight finished in 0.8s
```

It never calls your real providers, and exits non-zero on failure so it can gate
a deploy. A guarantee your configuration does not claim is reported as `SKIP`
with what to add, not silently passed.

---

## Architecture

Single process, single replica. Everything the scheduler needs is correct in
memory.

```
                 ┌──────────────────────────────────────────────┐
   client ──────▶│  GATEWAY                                     │
   (OpenAI-      │                                              │
    compatible)  │   auth ──▶ validate ──▶ estimate cost        │
                 │                              │               │
                 │                              ▼               │
                 │   ┌──────────────────────────────────────┐   │
                 │   │ ADMISSION + SCHEDULER                │   │
                 │   │  reserved floors │ shared pool       │   │
                 │   │  bounded queues  │ deadlines         │   │
                 │   │  weighted fair queueing, in tokens   │   │
                 │   └──────────────┬───────────────────────┘   │
                 │                  │ capacity lease            │
                 │                  ▼                           │
                 │   ┌──────────────────────────────────────┐   │
                 │   │ BUDGET LEDGER                        │   │
                 │   │  reserve ceiling │ clamp │ settle    │   │
                 │   └──────────────┬───────────────────────┘   │
                 │                  ▼                           │
                 │   ┌──────────────────────────────────────┐   │
                 │   │ PROVIDER ROUTER                      │   │
                 │   │  candidates │ phased failover        │   │
                 │   │  circuit breakers │ health           │   │
                 │   └──────────────┬───────────────────────┘   │
                 │                  ▼                           │
                 │   stream pump ──▶ SSE ──▶ settle ──▶ client  │
                 └──────────────────┬───────────────────────────┘
                                    ▼
                 ┌──────────────────────────────────────────────┐
                 │ PROVIDERS                                    │
                 │  synthetic fleet (built in, no API key)      │
                 │  + your own adapters                         │
                 └──────────────────────────────────────────────┘

   observability          instruments
   ─────────────          ───────────
   /metrics  Prometheus   scenarios    break it on purpose
   /v1/providers  health  verify       check your own config
   structured logs        top          live operational view
```

A capacity lease is an async context manager, so completion, provider failure,
client disconnect and unexpected exceptions all release through one path. A
failover reuses the same lease and the same budget reservation — a retry is
still one request competing for capacity.

## Observability

`/metrics` exposes per-tenant queue wait, queue depth, token throughput, budget
headroom, breaker state, failovers, terminal failures split by whether tokens
had already been delivered, and event-loop lag. The Grafana dashboard has eleven
panels covering traffic, scheduling, tenants and providers.

Logs are structured, `text` by default and `json` via `SWITCHYARD_LOG_FORMAT`.
Notable events log at INFO; one line per request at DEBUG. **Prompts, responses
and credentials are never logged** — the logging helper rejects those field
names outright.

```bash
make up   # gateway :8000, fleet :8100, prometheus :9090, grafana :3000
```

## Using a real provider

Everything above runs on the built-in synthetic fleet, which is why it needs no
API key. Switchyard speaks one upstream format — OpenAI chat completions — so
pointing it at a real provider is configuration, not code:

```toml
[gateway]
providers = ["openai", "local"]

[providers.openai]
base_url = "https://api.openai.com/v1"
api_key_env = "OPENAI_API_KEY"       # read from the environment, never stored
upstream_model = "gpt-4o-mini"

[providers.local]
base_url = "http://localhost:11434/v1"   # Ollama, vLLM, anything compatible

[routes]
fast = ["openai", "local"]           # fall back to local when OpenAI is down
```

The demo path and the production path run the same adapter — the synthetic fleet
is an OpenAI-compatible endpoint like any other, not a special case. Scheduling,
budgets, breakers and failover behave identically either way.

## Deployment

One process plus the synthetic fleet, or one process pointed at real providers.
Docker Compose is the supported path and is verified end to end: build, health,
authenticated scrape, provisioned dashboard.

`switchyard.toml` is mounted rather than baked into the image, so limits can
change without a rebuild. For anything real, set `SWITCHYARD_KEY_PEPPER` and
mint fresh keys — the committed ones become worthless the moment you do.

There is no Kubernetes, no Redis, no message broker. Nothing in the product
needs them, and a single replica is the honest scope.

## CLI

| | |
|---|---|
| `switchyard serve` | run the gateway |
| `switchyard check` | is my configuration valid, and what does it mean? |
| `switchyard top` | what is happening right now? |
| `switchyard scenario <name>` | show me what happens in a specific situation |
| `switchyard verify` | does my configuration behave as intended? |
| `switchyard keys mint <id>` | mint a tenant or `--admin` credential |

## What this is not

Not a production system. No HA, no secrets management, no compliance story, no
multi-region. It is a working gateway with the engineering properties above
demonstrated and measured, not a hosted service.

## Layout

```
src/switchyard/
  core/        config, auth, scheduler, queue policies, prediction, budgets, health, routing
  gateway/     HTTP surface and the SSE pump
  adapters/    provider implementations
  obs/         metrics and structured logging
  synthetic/   the provider fleet
  scenarios/   runnable demonstrations
  bench/       load generator and experiments
  verify.py    configuration verification
```

`docs/decisions.md` records why the system is built the way it is, including the
mistakes found along the way.

## Development

```bash
make check      # ruff, mypy, tests
make test-all   # including the slow scenario tests
make bench      # regenerate every figure in plots/
```

MIT licensed.

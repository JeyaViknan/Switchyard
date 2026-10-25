# Handover: Switchyard DevOps + Live Console

Owner: Jeya Viknan · Written 25 Oct 2026 · Deadline for a demoable version: **Mon 26 Oct, night** (interview Tue 27 Oct)

This document tells you (Claude Code) what to build around Switchyard so that it becomes a **deployed, monitored, self-updating product that costs $0**, with a **live web console** whose home page shows what the gateway is doing in real time. Read `CLAUDE.md` first: its hard rules override anything here.

---

## 0. Goal in one paragraph

Today Switchyard is a well-tested Python gateway that has never been deployed, and its CI has been red for two months. When you're done:

- every merge to `main` is tested, built into one image, scanned, pushed to GHCR by commit SHA, deployed to one free-tier EC2 instance over SSM, smoke-tested, and rolled back automatically if the smoke test fails;
- the instance runs five containers with Docker Compose: **gateway**, **synthetic fleet** (fake LLM providers), **console** (new web UI), **Prometheus**, **Grafana**;
- the console's home page is a live, animated system map (tenants → queues → capacity → providers, breakers, failovers) that Jeya can open on his iPhone during the interview, plus buttons that start guided demos (noisy neighbour, provider outage) and show PASS/FAIL verdicts as they run;
- Grafana alerts reach Jeya's phone through Telegram; Terraform builds and destroys the AWS side; everything costs $0.

**No real LLM API is used.** All traffic goes to the synthetic fleet, and the UI says so ("Simulation mode"). The same build must work with real OpenAI-compatible providers by changing configuration only, and the docs must show a company how (section 9).

---

## 1. Current state (verified 25 Oct 2026)

### Repository
- `main` @ `f9e885f` "Read the release version from the built wheel". Tag `v0.1.0` points at the same commit.
- Python ≥ 3.12 (Dockerfile and CI use 3.14). FastAPI + uvicorn (uvloop, httptools), httpx, prometheus-client, orjson, numpy.
- Layout: `core/` (config, auth, scheduler, queueing, prediction, budget, health, routing, persistence), `gateway/` (HTTP + SSE pump), `adapters/` (OpenAI-compatible client), `obs/` (metrics, logs), `synthetic/` (fleet with fault-injection control API), `scenarios/`, `bench/` (open-loop load generator), `verify.py`, `cli/`.
- Tests: ruff clean, mypy clean, **356 passed** locally on Python 3.13 and 3.14 in a clean clone (`pytest -q`, about 60–70 s).
- Dev deployment: `Dockerfile` (python:3.14-slim, non-root uid 10001, HEALTHCHECK on `/health`, one image, two entrypoints), `docker-compose.yml` (gateway, fleet, Prometheus 1 s scrape, Grafana with anonymous **Admin**, named volume for spend state). Prometheus and Grafana images are `:latest`.
- Observability: admin-protected `/metrics`, 11-panel Grafana dashboard (`deploy/grafana/dashboards/switchyard.json`, `[1m]` rate windows), JSON logs with a prompt-redaction guard. **No alert rules.**

### GitHub Actions: broken
- **CI has failed on every push since 28 Aug** (runs #2–#9). The only green run is #1 (27 Aug).
- Latest failure, run `38950579817` on `f9e885f`: job `check`, **step 7 `pytest -q` exited with code 2 after about 4 seconds**. Ruff and mypy passed. Exit code 2 after 4 s means pytest stopped during collection or startup, not a failing assertion. The same commit passes locally in a clean environment, so the cause is specific to the runner.
- Release workflow runs #1 and #2 (tag `v0.1.0`) also failed (most likely at the same pytest step, since `verify` runs the same tests first), so **nothing was published**: no GitHub release, no GHCR image, almost certainly no PyPI upload. The README's `docker pull ghcr.io/jeyaviknan/switchyard:latest` does not work today.
- Annotations: Node 20 deprecation for `actions/checkout@v4` and `actions/setup-python@v5`; `ubuntu-latest` migrating to Ubuntu 26 from 19 Oct 2026.

### Existing APIs the console will use (real responses captured locally)
- `GET /health` → `{"status":"ok","auth":"required","policy":"drr","max_concurrency":24,"inflight":0}`; 503 with `"status":"draining"` while draining.
- `GET /v1/scheduler/stats` (admin) → `policy, max_concurrency, inflight, shared_pool{in_use,capacity}, queue_depth, tenants{<id>: weight, reserved_concurrency, max_concurrency, inflight, queued, predicted_output_tokens{model:n}, budget{limit, spent, reserved_in_flight, available, requests_remaining_estimate}}}`.
- `GET /v1/providers` (admin) → `{<name>: state ("closed"|"half_open"|"open"), successes, failures, errors{class:n}, ttft_ewma_ms, reopens_in_s, consecutive_trips, routes_serving[]}`.
- `GET /metrics` (admin) → Prometheus text. Useful series: `switchyard_requests_total{model,provider,outcome}` (`ok`|`error`), `switchyard_queue_wait_seconds` histogram `{tenant}`, `switchyard_dispatched_total{tenant}`, `switchyard_admission_rejected_total{tenant,reason}`, `switchyard_tenant_tokens_total{tenant}`, `switchyard_failovers_total{from_provider,to_provider}`, `switchyard_breaker_state{provider}` (0 closed, 1 half-open, 2 open), `switchyard_capacity_utilisation`, `switchyard_queue_depth`, `switchyard_ttft_seconds`.
- Fleet (no auth, must stay private): `GET /control/profiles`, `PUT /control/profiles/{provider}` with e.g. `{"faults":{"error_rate":1.0}}`, `POST /control/reset`, `PUT /control/seed`, `GET /health`. Default providers `fast` (TTFT ~220 ms, 90 tok/s) and `slow` (TTFT ~650 ms, 35 tok/s). Routes in `switchyard.toml`: `fast = ["fast","slow"]`, `slow = ["slow"]`.
- Auth: tenant keys are SHA-256 over a server pepper (`SWITCHYARD_KEY_PEPPER`, dev pepper if unset); `switchyard keys mint <tenant>` and `switchyard keys mint --admin` mint raw key + hash. `SWITCHYARD_ADMIN_KEY` env is read by the CLI.
- Config env: `SWITCHYARD_CONFIG`, `SWITCHYARD_STATE_FILE`, `SWITCHYARD_FLEET_URL`, `SWITCHYARD_LOG_FORMAT`, `SWITCHYARD_LOG_LEVEL`, `SWITCHYARD_KEY_PEPPER`.
- Shutdown: lifespan drains (refuses queued, waits for running up to `drain_timeout_s` = 30 s), then persists spend. Spend also flushes every `state_flush_interval_s` = 2 s.

---

## 2. Target architecture

```
                    Internet (security group: 80, 8000, 3000 only)
   Jeya's phone / laptop / interviewer        Beacon (external uptime check)       GitHub Actions
         |  :80 console   |  :8000 API   |  :3000 Grafana         | /readyz            | OIDC -> AWS role
         v                v              v                        v                    v
 +----------------------- EC2 t3.micro (free tier) · Docker Compose · Amazon Linux 2023 --------------------+
 |  console :8080->80  --polls-->  gateway :8000  --->  fleet :8100 (synthetic providers, private)          |
 |     |  drives demo traffic ------^   |  ^                                                                |
 |     |  injects faults -------------------------------> fleet /control (private network only)             |
 |     |                                |  | scrape /metrics (admin key from file)                         |
 |     |                           Prometheus (private, 15 s, 3 d)  <--- Grafana :3000 (read-only viewer)    |
 |     |                                                                    | alert rules -> Telegram bot    |
 |  /opt/switchyard: git checkout @ SHA (deploy/ files) · /opt/switchyard/state/state.json (spend)           |
 |  /etc/switchyard/{gateway,console,grafana}.env + scrape-key (root-only, written from SSM at deploy)       |
 +-----------------------------------------------------------------------------------------------------------+
   SSM Parameter Store (/switchyard/...)   CloudWatch alarm (status check -> auto-recover)   AWS Budgets ($0.01)
   GHCR: ghcr.io/jeyaviknan/switchyard:<sha>   (public package, pulled anonymously)
```

Principles to keep (and say in the interview):
- **One image, three entrypoints** (gateway, fleet, console). Build once, promote by SHA, never build on the server.
- **Single gateway process by design.** Deploys are *recreate with drain*, never rolling (two gateways would each claim all upstream capacity and both write the spend file).
- **Config and code move together.** The instance checks out the repo at the deployed SHA, so `deploy/` files and the image tag always match; rollback = previous SHA.
- **Console ≠ Grafana.** Grafana is the operator's metrics tool. The console is the product's face: a live story of fairness and failover, plus guided demos. It reads the same numbers.

---

## 3. Milestones (in priority order)

Each milestone: implement, test, update docs, write `docs/learning/M<n>.md`, commit. Stop at ⛔ checkpoints.
**Cut line:** if time is short, M0–M4 alone make a strong demo that runs locally (`make prod-local`). M5–M8 put it on AWS.

| # | Milestone | Rough size |
|---|---|---|
| M0 | Make CI green again | 30–60 min |
| M1 | Gateway ops refactors (health split, build info, provider kind) | ~1 h |
| M2 | Console backend: collector, API, traffic generator, faults, auth | 2–3 h |
| M3 | Console frontend: live map home page + other pages | 3–4 h |
| M4 | Production Compose + local prod run | ~1.5 h |
| M5 | CI: image build, scan, smoke test, publish by SHA; fix release | ~1 h |
| M6 | Terraform for the AWS side | ~1.5 h |
| M7 | Continuous deployment with automatic rollback | ~1.5 h |
| M8 | Grafana alerts to Telegram | ~45 min |
| M9 | Docs, runbooks, README honesty pass, release v0.2.0 | ~1 h |

---

## M0 — Make CI green again

1. ⛔ Ask Jeya to run `gh run view 38950579817 --log-failed` (or open the run in the browser while signed in) and paste the pytest output. You cannot see the logs without his GitHub session.
2. Reproduce on `ubuntu-latest` conditions (Python 3.14, fresh venv, `pip install -e ".[dev,bench]"`, `pytest -q`). Likely suspects, in order: a collection-time error under `filterwarnings = ["error::DeprecationWarning"]` from a newer dependency on the runner; a missing system library for a dependency; something in `tests/conftest.py` that behaves differently on the runner.
3. Fix the cause, not the symptom. Don't delete tests or drop the DeprecationWarning filter globally; if a third-party warning is the cause, filter that one warning by module with a comment explaining why.
4. Make CI reproducible: pin dev tools in a constraints file (`requirements-dev.lock` or `constraints.txt`) used by CI; print `python -VV` and `pip freeze` in the job for future debugging.
5. Upgrade actions off Node 20 (`actions/checkout`, `actions/setup-python`, upload/download-artifact to their current majors) and pin every third-party action to a commit SHA with the version in a comment. Pin `runs-on: ubuntu-24.04` so the Ubuntu 26 migration can't break builds silently.

**Done when:** CI is green on a branch push, and `docs/decisions.md` has an entry "Why CI was red for two months and what changed". This is a great interview story; write it plainly.

---

## M1 — Gateway ops refactors (small, product-safe)

1. **Split health:** `GET /livez` returns 200 whenever the process can answer (even while draining). `GET /readyz` returns 503 while draining (today's `/health` logic) and includes build info. Keep `GET /health` as an alias of `/readyz` for compatibility. The Dockerfile HEALTHCHECK uses `/livez`; the deploy smoke test, Beacon and Compose `depends_on` use `/readyz`.
2. **Build info:** Dockerfile `ARG GIT_SHA=dev` and `ARG VERSION`; `ENV SWITCHYARD_GIT_SHA=$GIT_SHA`; OCI labels `org.opencontainers.image.revision` and `.version`. `/readyz` returns `{"version": ..., "git_sha": ..., "started_at": ...}` alongside the existing fields. Add gauge `switchyard_build_info{version,git_sha} 1`.
3. **Provider kind:** `/v1/providers` adds `"kind": "synthetic"` when the provider resolves to the built-in fleet and `"external"` when it has a `[providers.<name>]` `base_url`. Never expose the URL or key env name. The console uses this for its Simulation/Live banner.
4. **Docker stop timing:** add `stop_grace_period: 40s` to the gateway in `docker-compose.yml` (drain is 30 s; Docker's default SIGKILL after 10 s cuts it short). Record in `docs/decisions.md`.
5. Pin the base image by digest in the Dockerfile (keep the human-readable tag in a comment).

**Tests:** `/livez` stays 200 during drain while `/readyz` is 503; build info appears in `/readyz` and the gauge; `kind` is right for synthetic and external providers.

---

## M2 — Console backend (`src/switchyard/console/`)

A separate FastAPI app in the same package and image: `uvicorn switchyard.console.app:create_app --factory --port 8080`. It holds the admin key **server-side** and never sends secrets to the browser.

### Modules
```
console/
  settings.py      # env-driven settings (pydantic is already available via FastAPI)
  collector.py     # 1 s poll loop -> Snapshot; 10-minute ring buffer; derived rates; events
  promtext.py      # parse /metrics text (prometheus_client.parser) -> counters/histograms; rate + p95 from deltas
  traffic.py       # capped open-loop traffic generator with per-tenant keys
  scenarios.py     # guided demos + live verdicts
  auth.py          # console password -> HMAC-signed session cookie (stdlib hmac, no new deps)
  app.py           # routes, SSE, static files, security headers
  static/          # index.html, app.css, app.js, map.js (no build step)
```

### Settings (env)
`CONSOLE_GATEWAY_URL` (http://gateway:8000), `CONSOLE_FLEET_URL` (http://fleet:8100), `SWITCHYARD_ADMIN_KEY`, `CONSOLE_DEMO_KEYS` (JSON `{"acme":"sk_sy_...","globex":"...","initech":"..."}`), `CONSOLE_PASSWORD`, `CONSOLE_SESSION_SECRET`, `CONSOLE_GRAFANA_URL`, `CONSOLE_PUBLIC_API_URL`, `SWITCHYARD_GIT_SHA`, `DEPLOYED_AT`, `DEPLOY_RUN_URL`. The console must start and show "gateway unreachable" if the gateway is down; never crash-loop.

### Collector
Every 1 s, concurrently: `/readyz`, `/v1/scheduler/stats`, `/v1/providers`, `/metrics` (admin), fleet `/control/profiles`, fleet `/health`, Prometheus `/-/ready`, Grafana `/api/health`. Short timeouts (1 s); a failed source marks that service down instead of failing the snapshot.

Derive per second from counter deltas (handle counter resets after a gateway restart):
- per tenant: `offered = dispatched + rejected`, `admitted = dispatched`, `rejected` (by reason), tokens/s, queue-wait p95 over the last 60 s from histogram bucket deltas (label it "approx. from histogram buckets");
- per provider: ok/s and failed/s from `requests_total{provider,outcome}`, breaker state, `reopens_in_s`, TTFT EWMA, injected fault from the fleet profile;
- failovers/s per `(from, to)` pair; overall rps, success ratio over 60 s, failovers in the last 5 min, capacity utilisation.

**Events** (kept as the last 200): breaker transitions (closed→open, open→half-open, half-open→closed), first failover after a quiet period, a tenant starting or stopping being rejected, budget below 10%, gateway draining or not ready, gateway back, a new `git_sha` seen (deploy), traffic profile started/stopped, fault injected/cleared. Each event has `at`, `kind`, `severity` (info|warning|critical), plain-English `text` ("Breaker for 'fast' opened after repeated 5xx; traffic now goes to 'slow'").

### Snapshot contract (what the browser receives)
```json
{
  "at": "2026-10-26T10:15:03Z",
  "mode": "simulation",
  "build": {"version": "0.2.0", "git_sha": "3f9c2a1", "deployed_at": "...", "run_url": "...", "uptime_s": 5231},
  "services": {"gateway": "ready", "fleet": "up", "prometheus": "up", "grafana": "up"},
  "gateway": {"policy": "drr", "max_concurrency": 24, "inflight": 11, "shared_pool": {"in_use": 5, "capacity": 12}, "queue_depth": 64, "draining": false},
  "tenants": [{"id": "acme", "weight": 3.0, "floor": 6, "ceiling": 18, "inflight": 6, "queued": 64,
               "offered_rps": 25.0, "admitted_rps": 2.4, "rejected_rps": 22.1, "queue_wait_p95_ms": 2100,
               "tokens_per_s": 480, "budget": {"limit": 2000000, "spent": 34200, "available": 1965800}}],
  "providers": [{"name": "fast", "kind": "synthetic", "state": "open", "reopens_in_s": 3.2, "ttft_ewma_ms": 231,
                 "ok_rps": 0.0, "failed_rps": 4.1, "fault": {"error_rate": 1.0}}],
  "failovers": [{"from": "fast", "to": "slow", "rps": 1.2}],
  "kpis": {"rps": 27.3, "success_ratio_60s": 0.998, "failovers_5m": 26, "capacity_utilisation": 0.46},
  "traffic": {"profile": "noisy-neighbour", "running": true, "ends_in_s": 74},
  "scenario": {"name": "provider-outage", "phase": "outage", "checks": [{"name": "...", "status": "pass|fail|pending", "detail": "..."}]},
  "events": [{"at": "...", "kind": "breaker_opened", "severity": "warning", "text": "..."}]
}
```
`mode` = `"simulation"` when every provider is `kind: synthetic`, otherwise `"live-providers"`.

### HTTP API
Public (read-only, no secrets): `GET /` (UI), `GET /static/*`, `GET /api/snapshot`, `GET /api/stream` (SSE: a snapshot every 1 s, events as they happen, heartbeat comment every 15 s), `GET /api/history?minutes=10` (downsampled series for sparklines), `GET /healthz`.
Protected (session cookie + header `X-Console: 1` on every POST as a simple CSRF guard): `POST /api/login`, `POST /api/logout`, `POST /api/traffic` `{"profile": "steady"|"noisy-neighbour"|"stop"}`, `POST /api/faults/{provider}` `{"error_rate": 0..1}` or `{"stall": true}` or `{"clear": true}`, `POST /api/scenarios/{name}/start`, `POST /api/scenarios/stop`, `POST /api/reset` (stop traffic, fleet `/control/reset`).
Security headers on everything: strict CSP (`default-src 'self'`), `X-Content-Type-Options`, `Referrer-Policy`, `frame-ancestors 'none'`. Login: constant-time compare, 5 attempts/min per IP. The cookie is HttpOnly + SameSite=Strict with a 12 h expiry; note that `Secure` is impossible on plain HTTP, and record that trade-off in decisions.

### Traffic generator (protects the t3.micro and the demo)
- Open-loop Poisson arrivals per tenant (same idea as `bench/loadgen.py`; reuse its request code where clean), streaming requests with the tenant's own key, `model: "fast"`, `max_tokens` 200–400.
- Profiles: `steady` (acme 1.5 rps, globex 1 rps, initech 0.3 rps), `noisy-neighbour` (globex 1 rps steady; after 10 s acme floods at 25 rps for 30 s, then back to steady).
- **Hard caps:** total ≤ 30 rps, ≤ 120 open streams, every profile auto-stops after 120 s, one profile at a time, stopping cancels in-flight work cleanly. Caps are constants with a comment explaining the t3.micro reason.

### Guided scenarios with live verdicts (same claims as the CLI scenarios)
1. **Noisy neighbour** (~60 s): steady → flood → steady. Checks: quiet tenant (globex) served ≥ 90% of what it offered during the flood; its queue-wait p95 < 500 ms; rejections fall only on the flooding tenant.
2. **Provider outage** (~60 s): steady traffic; at 10 s set `fast` error_rate 1.0 for 25 s, then clear. Checks: client success ≥ 95% during the outage; breaker for `fast` opened; failovers observed; breaker closed again after recovery.
3. **Reset**: stop traffic, clear faults.
Verdicts update live (pending → pass/fail) and end with a one-line summary.

### Tests
promtext parsing and rate/p95 from deltas (including counter reset); event derivation from snapshot pairs; mode detection; auth (bad password, expiry, tamper, missing CSRF header); traffic caps and auto-stop; a slow-marked end-to-end test that starts fleet + gateway + console in-process and runs the outage scenario to PASS.

---

## M3 — Console frontend (the demo centrepiece)

Plain HTML/CSS/JS (ES modules), SVG, one `EventSource('/api/stream')`. Hash routes: `#/` (Live map), `#/tenants`, `#/providers`, `#/demo`, `#/system`, `#/api`. Mobile-first: Jeya demos on an iPhone in portrait. Light and dark themes via CSS variables.

### Persistent chrome
- **Mode banner** on every page: `SIMULATION · synthetic providers · no real LLM calls · $0`, linking to "How to connect real providers" (`#/system`). In live-providers mode: `LIVE PROVIDERS` in a different style.
- Top KPI strip: requests/s, success % (60 s), failovers (5 min), capacity in use, version + short SHA, a connection dot (live / reconnecting).

### Home: Live system map (`#/`)
An SVG diagram that reads like an AWS console architecture view, animated from real numbers:
- **Left: tenant cards.** Name, weight, floor and ceiling, inflight/queued bars, budget ring, offered vs admitted rps.
- **Centre: the gateway.** A grid of `max_concurrency` capacity slots: reserved floors outlined per tenant, the shared pool marked, each slot filled in its tenant's colour from `inflight`. A queue bar per tenant feeds into it. Policy label (`drr`).
- **Right: provider cards.** Name, kind badge, breaker state as icon + text (✓ Closed / ◐ Half-open / ✕ Open, "retries in 3 s"), TTFT, ok/failed per second, an injected-fault badge ("error rate 100%").
- **Flows:** particles travel tenant → gateway → provider at a rate proportional to the measured rps, coloured per tenant. Rejected traffic bounces off the gateway edge in red. During failover, an arc animates from the failing provider to the fallback. Label honestly in a small legend: *"Dots show measured rates, sampled every second; each dot ≈ N requests."*
- **Bottom: live event feed** (newest first, severity icon + plain-English text + time).
- Phone layout: the same diagram stacked top-to-bottom (tenants → gateway → providers).
- `prefers-reduced-motion`: no particles; numbers and states only. Cap rendering at 30 fps; pause animation when the tab is hidden.

### Other pages
- **Tenants:** table: weight, floor, ceiling, inflight, queued, offered/admitted/rejected rps, queue-wait p95, tokens/s, budget bar with requests-remaining estimate; a sparkline per tenant from `/api/history`.
- **Providers:** table plus a 10-minute breaker-state timeline per provider (closed/half-open/open bands), failover counts.
- **Demo** (needs login): big buttons for Noisy neighbour, Provider outage, Steady traffic, Stop/Reset, plus manual fault toggles per provider. Each button explains in one line what will happen and what to watch on the map. Live checklist of verdicts with ✓/✕/… while it runs.
- **System:** service health tiles (gateway ready/draining, fleet, Prometheus, Grafana), build info (version, SHA linked to the commit, deployed-at, uptime, link to the GitHub Actions run), infrastructure facts (EC2 t3.micro, region, Compose, cost $0 / free tier), links to Grafana and the GitHub repo, and a short "How to connect real providers" explainer linking to `docs/production.md`.
- **API:** how to call the gateway (`curl` and OpenAI SDK snippets using `CONSOLE_PUBLIC_API_URL`, key shown as `<your-tenant-key>`), endpoint table from the README.

**Done when:** on a phone and a laptop, with `make up`, the map shows idle state; "Noisy neighbour" visibly floods acme while globex stays green and both verdicts pass; "Provider outage" flips `fast` to Open, arcs failover to `slow`, success stays ≈100%, then recovers. Save before/after screenshots to `docs/images/` for the README.

---

## M4 — Production Compose + local prod run

Create `deploy/`:
```
deploy/
  compose.prod.yml            # 5 services, images by ${IMAGE_TAG}, no builds
  compose.local.yml           # override for laptops: build: .., env files from deploy/local/
  switchyard.prod.toml        # tenants acme/globex/initech with HASHES minted with the prod pepper (no raw keys)
  switchyard.company.example.toml   # real providers example (section 9)
  prometheus.prod.yml         # 15 s scrape; authorization.credentials_file: /etc/prometheus/scrape-key
  grafana/provisioning/{datasources,dashboards,alerting}/   # reuse the existing dashboard JSON
  deploy.sh  smoke.sh
scripts/mint-prod-keys.sh     # Jeya runs it; writes raw secrets to SSM, prints hashes for the prod toml
scripts/dev-secrets.sh        # writes deploy/local/*.env from the committed DEV keys (git-ignored output)
```

`compose.prod.yml` requirements:
- `gateway`: `ports: ["8000:8000"]`, `env_file: /etc/switchyard/gateway.env` (pepper only), `SWITCHYARD_CONFIG`, `SWITCHYARD_STATE_FILE=/var/lib/switchyard/state.json`, `SWITCHYARD_FLEET_URL=http://fleet:8100`, JSON logs, volume `/opt/switchyard/state:/var/lib/switchyard`, `stop_grace_period: 40s`, healthcheck on `/livez`, `restart: unless-stopped`.
- `fleet`: no published ports.
- `console`: `ports: ["80:8080"]`, `env_file: /etc/switchyard/console.env` (admin key, demo keys, console password, session secret), `depends_on: gateway: condition: service_healthy`.
- `prometheus`: no published ports, `--storage.tsdb.retention.time=3d`, mounts the scrape-key file read-only.
- `grafana`: `ports: ["3000:3000"]`, anonymous **Viewer**, `env_file: /etc/switchyard/grafana.env` (admin password, Telegram token + chat id), provisioning mounted read-only.
- **Separate env file per service** (least privilege): the gateway never sees the console password; Grafana never sees the pepper.
- Every service: pinned image versions, `json-file` logging with `max-size: 10m`, `max-file: 3`.
- Memory: the box has 1 GB RAM plus 2 GB swap. Run `make prod-local` under the noisy-neighbour profile, record `docker stats` peaks in `docs/learning/M4.md`, and only then decide whether to set `mem_limit`s. Don't guess.

`make prod-local`: builds the image locally (`IMAGE_TAG=local`), generates dev env files, runs `compose.prod.yml` + `compose.local.yml`. Console on http://localhost, API on :8000, Grafana on :3000. **This is also the demo fallback if AWS or the venue Wi-Fi fails.**

Also add the console to the dev `docker-compose.yml` so `make up` shows it.

---

## M5 — CI: image, scan, smoke, publish

Extend `.github/workflows/ci.yml` (keep it one readable file):
1. `check` (existing, now green): ruff, mypy, pytest, plus `switchyard check --config deploy/switchyard.prod.toml` (set a throwaway pepper in the job env so the hashes parse; check reports structure, not secrets).
2. `image`: build with buildx and GHA cache (`--build-arg GIT_SHA=${{ github.sha }}`), load locally, **Trivy** scan (fail on HIGH/CRITICAL that have a fix; document any ignore with a reason and expiry).
3. `smoke`: `make prod-local`-style stack in CI with dev secrets; wait for health; one completion through the gateway; `/readyz` reports the SHA; console `/api/snapshot` returns 200; `switchyard verify` (~35 s); tear down with `-v`.
4. `publish` (only `push` to `main`): push `ghcr.io/jeyaviknan/switchyard:<full-sha>` and `:main`. `linux/amd64` is enough for the t3.micro; the release workflow stays multi-arch.
5. `terraform` (when `infra/**` changes): `terraform fmt -check`, `init -backend=false`, `validate`.

Fix `release.yml`: it must succeed end to end on the next tag. Gate the PyPI job behind a repository variable (`if: vars.PUBLISH_PYPI == 'true'`) so a missing PyPI trusted-publisher setup doesn't block the GHCR image and GitHub release; make `github-release` depend on `image` only.
⛔ After the first publish, Jeya sets the GHCR package `switchyard` to **Public** (package settings), so the instance can pull without credentials.

---

## M6 — Terraform (`infra/`)

Terraform ≥ 1.10, AWS provider `~> 6.0`, region `ap-south-1` (variable). **Local state** (git-ignored); add a commented S3 backend block with `use_lockfile = true` as "what a team would do".
```
infra/
  versions.tf  variables.tf  outputs.tf
  network.tf     # data: default VPC + a default subnet; SG ingress 80, 8000, 3000 from 0.0.0.0/0; egress all; NO 22
  compute.tf     # t3.micro, AL2023 AMI via SSM public parameter, credit_specification { cpu_credits = "standard" },
                 # root 30 GB gp3 encrypted, IMDSv2 required, user_data (below), tags project=switchyard
  iam.tf         # instance role: AmazonSSMManagedInstanceCore + read ssm:GetParametersByPath on /switchyard/*
                 # GitHub OIDC provider + role "switchyard-deploy": trust repo:JeyaViknan/Switchyard:environment:production;
                 # allow ssm:SendCommand (this instance + AWS-RunShellScript doc), ssm:GetCommandInvocation, ec2:DescribeInstances
  params.tf      # SecureString names only, value "CHANGE_ME", lifecycle { ignore_changes = [value] }:
                 # /switchyard/gateway/SWITCHYARD_KEY_PEPPER
                 # /switchyard/console/{SWITCHYARD_ADMIN_KEY,CONSOLE_DEMO_KEYS,CONSOLE_PASSWORD,CONSOLE_SESSION_SECRET}
                 # /switchyard/grafana/{GF_SECURITY_ADMIN_PASSWORD,TELEGRAM_BOT_TOKEN,TELEGRAM_CHAT_ID}
  monitoring.tf  # CloudWatch alarm StatusCheckFailed_System -> arn:aws:automate:<region>:ec2:recover;
                 # aws_budgets_budget: monthly COST limit 0.01 USD, email var.alert_email on ACTUAL > 0.01
```
`user_data` (idempotent): `dnf install -y docker git`; enable docker; install the Docker Compose v2 plugin from its GitHub release (pin the version; verify the checksum); 2 GB swap file in fstab; `git clone https://github.com/JeyaViknan/Switchyard /opt/switchyard`; `mkdir -p /opt/switchyard/state /etc/switchyard` with root-only permissions.
Outputs: `instance_id`, `public_ip`, `console_url`, `api_url`, `grafana_url`, `deploy_role_arn`.

Why each choice (put in `docs/learning/M6.md`): standard CPU credits (T3 defaults to *unlimited*, which can bill surplus credits under sustained load; standard throttles instead, so it stays $0); no Elastic IP (charged when not attached; the deploy finds the IP by instance id); AWS-managed SSM key (a customer-managed KMS key costs money); no SSH (SSM Session Manager instead).

⛔ **Jeya:** checks the account's Free Tier type (Billing → Free Tier), runs `terraform plan` then `apply`, runs `scripts/mint-prod-keys.sh` (fills SSM, prints hashes; you then commit the hashes into `deploy/switchyard.prod.toml`), creates the Telegram bot, and sets GitHub repo variables `AWS_REGION`, `AWS_DEPLOY_ROLE_ARN`, `INSTANCE_ID` plus a `production` environment.

---

## M7 — Continuous deployment

Add a `deploy` job to `ci.yml` after `publish`: `environment: production`, `concurrency: {group: deploy-production, cancel-in-progress: false}`, `permissions: id-token: write, contents: read`.
1. `aws-actions/configure-aws-credentials` with `role-to-assume: ${{ vars.AWS_DEPLOY_ROLE_ARN }}` (OIDC, no stored keys).
2. `aws ssm send-command --document-name AWS-RunShellScript --instance-ids $INSTANCE_ID` with: `cd /opt/switchyard && git fetch --depth 1 origin <sha> && git checkout -q <sha> && deploy/deploy.sh <sha> <run_url>`.
3. `aws ssm wait command-executed`, then print `get-command-invocation` stdout/stderr into the job log; fail the job if the status isn't `Success`.
4. External smoke from the runner against the public IP (looked up with `ec2 describe-instances`): console `/api/snapshot` 200, gateway `/readyz` reports this SHA.

`deploy/deploy.sh <sha> [run_url]` (runs as root on the instance, `set -euo pipefail`, idempotent):
1. Read `/switchyard/<service>/*` parameters with `aws ssm get-parameters-by-path --with-decryption`; write `/etc/switchyard/<service>.env` (mode 600) and `/etc/switchyard/scrape-key`. Add the non-secret values the console needs: `SWITCHYARD_GIT_SHA`, `DEPLOYED_AT`, `DEPLOY_RUN_URL`, and the public IP (from IMDSv2) for `CONSOLE_GRAFANA_URL` and `CONSOLE_PUBLIC_API_URL`.
2. `IMAGE_TAG=<sha> docker compose -f deploy/compose.prod.yml pull`, then `up -d --wait` (recreates the gateway: it drains, then the new one starts).
3. `deploy/smoke.sh <sha>` against localhost: `/readyz` 200 with the matching SHA, one streaming completion with a demo key, console and Grafana health.
4. Success → write the SHA to `/opt/switchyard/.current-sha`. Failure → check out the previous SHA, bring it up again, smoke it, exit non-zero (the workflow fails loudly with both logs).
5. `docker image prune -f --filter "until=72h"` so old images don't fill the 30 GB disk.

**Done when:** a trivial commit merged to `main` shows up live within minutes with the new SHA on the console's System page; a deliberately broken build (on a throwaway branch merged and reverted) triggers the automatic rollback, and the log proves it. Record deploy duration and downtime (expect a few seconds of 503 during the drain) in `docs/learning/M7.md`.

---

## M8 — Alerts to Jeya's phone

Grafana-managed alert rules provisioned from `deploy/grafana/provisioning/alerting/`, Telegram contact point using `${TELEGRAM_BOT_TOKEN}` / `${TELEGRAM_CHAT_ID}` from the env file:

| Alert | Expression | For | Severity |
|---|---|---|---|
| GatewayDown | `up{job="switchyard-gateway"} == 0` | 1m | critical |
| ClientErrorRateHigh | `sum(rate(switchyard_requests_total{outcome="error"}[5m])) / sum(rate(switchyard_requests_total[5m])) > 0.05` | 5m | critical |
| ProviderBreakerOpen | `switchyard_breaker_state == 2` | 2m | warning |
| CapacitySaturated | `switchyard_capacity_utilisation > 0.9` | 10m | warning |

Messages are plain English with a link to the console. Add a build-info panel to the dashboard (`switchyard_build_info`). Breaker open is only a *warning* because failover protects users; write that reasoning into the rule description.
⛔ Jeya adds `http://<ip>:8000/readyz` and `http://<ip>/` to Beacon as external monitors.

---

## M9 — Docs, runbooks, honesty pass, release

- `docs/deploy.md`: architecture diagram (ASCII is fine), how a deploy works, how to roll back by hand, how to destroy everything, cost table showing $0 and why.
- `docs/production.md`: section 9 below, expanded.
- `docs/runbooks/`: `provider-outage.md`, `gateway-down.md`, `state-file.md` (corrupt or lost spend). Each: symptoms → which console/Grafana panel to look at → commands → how to confirm it's fixed.
- `docs/demo-live.md`: the 5-minute interview demo script (section 8).
- README: replace "Not yet tagged" and "Container image" claims with the truth; add a "Live demo" section with screenshots; keep the "What this is not" section honest (single process, synthetic traffic, plain HTTP on the free setup).
- `CHANGELOG.md` entry; then ⛔ Jeya tags `v0.2.0` and the fixed release workflow publishes.

---

## 8. The demo this must support (5 minutes)

1. Phone: open `http://<ip>/`. Banner says Simulation; map idle; System page shows the SHA that GitHub deployed.
2. Demo page → **Noisy neighbour**: acme's lane floods, rejections bounce red, globex stays green, verdicts turn ✓.
3. **Provider outage**: `fast` flips to ✕ Open, failover arcs to `slow`, success stays ≈100%, Telegram alert arrives (ProviderBreakerOpen fires after 2 minutes, so keep the outage running long enough, or show the "pending" state in Grafana), then recovery.
4. Grafana on :3000: the same story in metrics.
5. GitHub Actions: the run that deployed this SHA; explain recreate-with-drain and the automatic rollback.

---

## 9. Running it for real (what a company would change)

Write this up as `docs/production.md` and ship `deploy/switchyard.company.example.toml`:
- Real providers: `[providers.openai] base_url = "https://api.openai.com/v1"`, `api_key_env = "OPENAI_API_KEY"`, `upstream_model = "gpt-4o-mini"`; a self-hosted fallback (`[providers.local] base_url = "http://vllm:8000/v1"`); routes `fast = ["openai", "local"]`. Provider keys go into SSM and the gateway's env file. Nothing else changes: same image, same pipeline. The console banner switches to LIVE PROVIDERS automatically.
- Remove the synthetic fleet service and the console's traffic/fault controls (an env flag `CONSOLE_DEMO_CONTROLS=false` hides and disables them).
- Size `max_concurrency` to what the provider contract allows, floors by Little's law (`switchyard verify` prints the arithmetic), budgets per tenant, `state_file` on durable storage.
- With a budget: HTTPS at a load balancer with an ACM certificate; ECS with built-in blue/green; spend moved out of the process (DynamoDB) so the new version starts from current spend; multiple replicas only after admission state is shared (Redis behind the existing `Scheduler` seam).

---

## 10. Interview notes to generate along the way (`docs/learning/`)

At minimum, make sure the notes answer: why one EC2 instance and not ECS/EKS ($0 + single-process design); why recreate-with-drain and not rolling; how the pipeline proves a deploy worked; how secrets flow (SSM → root-only env files per service, OIDC, no stored keys); why standard CPU credits; why breaker-open is a warning but error rate is critical; why the console reads the same numbers as Grafana; what broke (red CI, the 10 s vs 30 s stop timeout, the config-not-in-image story already in `docs/decisions.md`) and what changed.

## 11a. Pending verification

Carried forward until someone actually runs it:

| What | Why it is unverified | How to clear it |
|---|---|---|
| **Docker image build** | The Docker daemon has been down on the dev machine since M1. The Dockerfile changed substantially in M1 — base image pinned by digest, `ARG GIT_SHA`/`VERSION`, OCI labels, HEALTHCHECK moved to `/livez` — and none of it has been built. | `docker build -t switchyard:test .` then `make up`. M5 also builds it in CI, which will catch it independently. |
| **Compose stack end to end** | Same reason. `docker compose config` parses (client-side only); no container has been started. | `make up`, then check `/readyz`, the Grafana dashboard, and `stop_grace_period` behaviour on `make down`. |

Everything else claimed as done has been executed and its output recorded.

## 11. Status (update as you go)

| Milestone | Status | Notes |
|---|---|---|
| M0 CI green | **done** | Both jobs green (run 40191532932). Three bugs, one theme — the build assumed a dev machine: CI ran `pytest` vs the Makefile's `python -m pytest` (only the latter puts the repo root on `sys.path`, so collection aborted and **no test had run since 28 Aug**); the Makefile hardcoded `.venv/bin/python`, which CI lacks; the bench printer formatted a `None` lag ratio. Runner and all action SHAs pinned, Node 20 actions upgraded. 357 tests. |
| M1 Gateway ops refactors | **done** | `/livez` (200 while draining, reads no scheduler state) vs `/readyz` (503, carries version/git_sha/started_at/uptime); `/health` kept as an alias. `switchyard_build_info` gauge. `/v1/providers` reports `kind` without leaking base_url or key env. `stop_grace_period: 40s` — Docker's 10s SIGKILL was cutting the 30s drain to a third. Base image pinned by multi-arch digest, `ARG GIT_SHA`/`VERSION` + OCI labels. 16 new tests; one timing test marked `slow` and `make test-all` split into two processes, no assertions weakened. |
| M2 Console backend | **done** | `src/switchyard/console/`: settings, collector (1s poll, reset-safe rates, windowed p95 from bucket deltas), promtext, capped open-loop traffic (30 rps / 120 streams / 120s, enforced in `validate()`), guided scenarios with live verdicts, HMAC session auth + CSRF header, SSE. Read-only public, controls behind a session, admin key never leaves the process. 93 tests incl. an end-to-end outage demo against the synthetic fleet. Four fleet-control bugs found and regression-tested. |
| M3 Console frontend | **done** | Four static files, no build step, strict CSP (asserted by tests: no inline style/script/handlers, same-origin only). Live SVG map — tenant cards, capacity drawn as per-tenant slots, provider breaker cards, rate-proportional particles, failover arcs, event feed. Pages for tenants, providers, demo, system, API. Verified in a browser against the running stack; screenshots in `docs/images/`. Three browser-only bugs found and fixed (SSE opened pre-cookie undid login; buttons rebuilt 60×/min lost clicks; text overlap at the shipping width) plus a scenario check that reported FAIL when it meant no-evidence. |
| M4 Production Compose | not started | |
| M5 CI image/scan/publish | not started | |
| M6 Terraform | not started | |
| M7 Continuous deployment | not started | |
| M8 Alerts | not started | |
| M9 Docs + release | not started | |

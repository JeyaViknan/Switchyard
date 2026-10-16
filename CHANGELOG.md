# Changelog

Notable changes to Switchyard. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Switchyard is pre-1.0: the configuration file and the admin API may change
between minor versions. The OpenAI-compatible request surface will not.

## [Unreleased]

### Added
- **Durable tenant spend.** `gateway.state_file` keeps per-tenant token
  consumption across restarts, written atomically and flushed on an interval
  and during drain. Without it budgets reset every time the process does,
  which `switchyard check` now reports rather than leaving to be discovered.
- `SWITCHYARD_STATE_FILE` environment override, so one configuration file can
  be mounted into a container that has a volume for state and run from a
  checkout that does not.
- A `budgets survive a restart` check in `switchyard check`.
- Published container images on GHCR and a release workflow covering PyPI,
  images and GitHub releases.

### Changed
- The distribution is named `switchyard-gateway` on PyPI; `switchyard` was
  already taken. The installed command is still `switchyard`.
- The container runs as a non-root user and installs the package normally
  rather than in editable mode.

## [0.1.0] - 2026-08-28

First complete version: the scheduler, the reliability layer, and the harness
that demonstrates both.

### Added
- **Scheduling.** Weighted fair queueing across tenants on per-tenant virtual
  clocks, denominated in tokens rather than requests, with reserved
  concurrency floors, per-tenant ceilings and a work-conserving shared pool.
  Admission control with bounded queues and deadlines.
- **Budgets.** Three-way separation of estimated work, reserved ceiling and
  actual consumption, with `max_tokens` clamping so a tenant's spend cannot
  exceed its limit even when the estimate is wrong.
- **Reliability.** Timeouts decomposed into connect, first-token, inter-token
  and total, each producing a distinct error class; circuit breaking with a
  rolling window, jittered doubling cooldown and bounded half-open probes;
  phased failover that is transparent before the first token and a typed
  terminal error after it; graceful drain.
- **Providers.** Any OpenAI-compatible upstream (OpenAI, Together, Groq,
  OpenRouter, Ollama, vLLM), plus a deterministic synthetic fleet that runs
  through the same adapter.
- **The harness.** An open-loop, coordinated-omission-free load generator; the
  `noisy-neighbour` and `provider-outage` scenarios; `switchyard verify`,
  which runs your own configuration against the synthetic fleet and reports
  what it measured.
- **Operations.** A CLI (`serve`, `check`, `verify`, `scenario`, `top`,
  `keys`), Prometheus metrics, a provisioned Grafana dashboard, structured
  logging with a redaction guard, and a Docker Compose stack.
- **Evidence.** Reproducible benchmarks for baseline latency, fairness under
  contention and behaviour through a provider outage, regenerated end to end
  by `make bench`.

[Unreleased]: https://github.com/JeyaViknan/Switchyard/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/JeyaViknan/Switchyard/releases/tag/v0.1.0

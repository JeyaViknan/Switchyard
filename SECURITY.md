# Security

## Reporting a vulnerability

Please report security issues privately through
[GitHub's private vulnerability reporting](https://github.com/JeyaViknan/Switchyard/security/advisories/new)
rather than opening a public issue. Expect an acknowledgement within a few
days.

## What Switchyard does protect

- **Tenant credentials** are stored as SHA-256 digests over a server pepper,
  never in plaintext, and compared in constant time.
- **Operational endpoints** (`/metrics`, `/v1/scheduler/stats`,
  `/v1/providers`, `/v1/admin/*`) require a separate admin credential. They
  expose every tenant's usage and can take the gateway out of service, so no
  single tenant key reaches them.
- **Provider credentials** are read from the environment at request time and
  never written to a configuration file, a log or a metric.
- **Logs and metrics never carry request content.** The logging layer rejects
  a known set of field names outright, so logging a prompt is a loud failure
  rather than a silent leak. Metric labels are bounded sets; request ids are
  never labels.
- **Budgets are a hard bound.** A tenant's spend cannot exceed its limit even
  when output-length prediction is wrong, because the reservation is the
  request's ceiling and `max_tokens` is clamped to the headroom that remains.

## What it does not

Stated plainly, because a security section that implies more than it delivers
is itself a problem:

- **No TLS.** Run it behind a terminating proxy.
- **No secrets manager integration.** Provider keys come from the environment.
- **No per-tenant encryption, no audit log, no RBAC.** There are two roles:
  tenant and admin.
- **No protection against a malicious tenant's prompt content.** Switchyard
  schedules and meters requests; it does not inspect or filter them.
- **Single process, no HA.** A crash is an outage, bounded by however your
  supervisor restarts it. Spend survives if `gateway.state_file` is set;
  in-flight requests do not.
- **The committed development keys are public.** They exist so a fresh
  checkout can issue a request immediately, are minted against a built-in
  development pepper, and are worthless anywhere that sets
  `SWITCHYARD_KEY_PEPPER`. Set it, and mint your own keys, before exposing a
  gateway to anything.

## Supported versions

Pre-1.0, only the latest release receives fixes.

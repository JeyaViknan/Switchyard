# Contributing

Thanks for looking. Switchyard is small and opinionated, so the fastest way to
get a change merged is to know what it optimises for.

## What this project optimises for

**Measured claims over features.** Every component should either enforce an
invariant or produce a number. A feature that does neither is surface area.
If you cannot name the graph or the test that would show your change works,
that is worth resolving before writing it.

**Honest failure.** A silently truncated stream, a budget that is "usually"
enforced, or a benchmark that flatters the system are all worse than the
absence of the feature. `docs/decisions.md` records several cases where a
first implementation was wrong in exactly this way and what replaced it.

**Deliberate smallness.** There is no Redis, no message broker, no
orchestrator. Each was considered and rejected with a reason. Adding one is a
conversation, not a pull request.

## Getting set up

```bash
make install      # venv + dev and bench extras
make check        # ruff, mypy, 356 tests -- about a minute
```

`make test-all` adds the scenario end-to-end tests, which start real services
and drive seconds of load. They are excluded from the default run to keep the
edit-test loop fast, not because they are optional.

## Before opening a pull request

```bash
make check
```

CI runs exactly this. Specifically:

- **ruff** across `src` and `tests`.
- **mypy** with `disallow_untyped_defs` on `switchyard.core` and
  `switchyard.adapters` — the scheduler, budgets and routing are where a type
  error costs real money, so they are strict and the instruments are not.
- **pytest**. New behaviour needs a test. A bug fix needs the test that would
  have caught it.

Never weaken a test to make the suite pass. If a test exposes a design
problem, say so in the pull request — that is a more useful contribution than
the patch that hides it.

## Benchmarks

`make bench` regenerates every figure in `plots/` from scratch. CI does not run
it on push: a shared runner is too noisy for latency numbers to mean anything.
If a change should move a graph, run the benchmark on a quiet machine and say
what moved and by how much.

## Commit messages

Describe what changed and why, in the imperative. The history is meant to be
readable as an account of how the system got here.

## Reporting bugs

Open an issue with the configuration (redact the digests), what you expected,
what happened, and the output of `switchyard check`. If it involves load,
`switchyard verify` output helps more than a description.

# Engineering decisions

Decisions where the reasoning is not obvious from the code, recorded when made.

## Week 1 — the measurement rig

**The rig was built before any gateway feature.**
A harness written after the system it measures gets shaped by the results it
finds. Building it first also means the baseline exists to compare against;
without it, no later scheduling change can be shown to have done anything.

**Load generation is open-loop.**
Closed-loop generation cannot express offered load above what the system
absorbs, so queueing never appears in the measurement. Every latency is measured
from the request's intended start, and `scheduling_lag` is reported on every run
so a run in which the generator itself lagged is visibly invalid rather than
silently wrong.

**Request ids are derived from `(seed, index)`, not `uuid4()`.**
The fleet keys every draw on `(run_seed, request_id)`. With random ids the fleet
was deterministic in isolation while the workload was not: two runs of the same
spec produced different output lengths, so comparing two scheduling policies
would have compared two different workloads. This was found in review after the
reproducibility claim had already been written down, which is the argument for
testing a property rather than asserting it -- the fix is two lines and the test
that proves it is worth more than the fix.

**The synthetic fleet draws all randomness once, up front, into a `RequestPlan`.**
Drawing lazily as the stream progresses would make the draw sequence depend on
timing and on which faults happen to be enabled, so enabling a fault would
silently change a request's output length too — and an experiment comparing a
faulted run to a clean one would be comparing two different workloads. A test
pins this property.

**Non-streaming responses are assembled from the streaming path.**
There is exactly one code path to a provider. A separate non-streaming
implementation would double the failure modes to reason about and test, for a
response shape the caller could assemble itself. The synthetic fleet therefore
implements only streaming and rejects `stream: false` loudly.

**Every stream terminates with an explicit frame.**
A stream that simply stops is indistinguishable from a truncated one. Provider
failures become a typed terminal frame carrying `tokens_emitted` — how much
actually reached the client — rather than a dropped connection. The adapter
detects a provider stream that ended without `[DONE]` and converts it into that
explicit failure rather than reporting a clean completion.

**Unsupported request fields are rejected by name.**
A client that asked for tool calls and received a plain completion has been given
a wrong answer, not a degraded one. A bug found by this rule's own test:
`body.get("max_tokens") or DEFAULT` treats an explicit `0` as absent and
substitutes the default — the exact silent-ignore behavior the validation exists
to prevent.

**The httpx connection pool is sized above any gateway concurrency limit.**
If the pool were smaller, requests would queue *inside httpx*, invisible to the
scheduler — which would then be fairly allocating a resource it does not
actually control. The scheduler must always be the binding constraint.

**Request time is decomposed into disjoint spans that sum to the total.**
`total = queue_wait + provider_time + gateway_overhead`, with overhead defined
as the *residual* so the decomposition cannot silently fail to add up. The first
version computed overhead as `total - (last_token_at - started)`, which reduces
algebraically to the gap after the final token -- a sub-millisecond serialization
cost that always looked excellent and measured nothing. Its help text claimed it
was "the latency Switchyard is responsible for."

Two consequences shaped the fix. The timeline is created at request arrival,
before parsing and routing, rather than inside the streaming generator; had it
stayed where it was, week 2's queue wait would have fallen outside the measured
span entirely and the scheduler would have appeared free. And `queue_wait` is a
value the admission controller records, not a difference between two marks: a
derived value would let ordinary parse time drift into it, and would read as
zero if some future path forgot to set a mark. As an owned value, zero means
nothing queued.

What overhead excludes, deliberately: per-chunk pump work interleaved with
provider waits. Separating that would require timestamping around every chunk,
a measurement whose cost is comparable to the quantity measured. `event_loop_lag`
is where pump CPU cost surfaces instead.

**The load generator runs in a separate process from the system under test.**
The first version ran fleet, gateway, and generator on one event loop. Scheduling
lag reached 20ms at 20 rps -- the instrument was delaying the thing it measured.
With subprocess isolation the same measurement runs at 0.0-2.5% of median
latency through 40 rps. Generator health is judged as a ratio of lag to median
latency rather than a fixed threshold, because the same absolute lag is
negligible for a 2s request and disqualifying for a 50ms one.

**Metrics carry only bounded labels.**
Provider and model come from configuration; outcome is a closed enum. Request id
is never a label. One unbounded label is enough to make a metrics backend
unusable.

**`event_loop_lag` is instrumented from day one.**
In an asyncio gateway, CPU-bound work on the loop is the dominant source of tail
latency that is not the provider's fault. Measuring it from the start means a
later p99 regression can be attributed rather than guessed at.

**Histogram buckets are explicit.**
Prometheus defaults top out at 10s, which would put most LLM generations in
`+Inf` and make p99 unmeasurable. Latency and gateway-overhead use separate
bucket sets because they differ by three orders of magnitude.

**Inter-token gaps are persisted as samples, not as a per-request mean.**
Inter-token latency is a tail metric: a stream whose mean gap is fine but whose
p99 gap is 400ms reads as stuttering. Pooling per-request means makes that
percentile uncomputable, and the data cannot be recovered without re-running the
experiment.

**Python 3.14, venv + pip.**
3.14 is the only interpreter on this machine meeting the project's `>=3.12`
floor. `uv` would be faster but is not installed, and adding a global tool to
the developer's machine is not something the project needs. All dependencies
have 3.14 wheels.

**Redis and Postgres are in the compose file but unused.**
Staged for the tenancy and accounting work so the stack does not change shape
mid-project. Nothing connects to them yet.


## Week 2 -- the scheduler

**The scheduler is in-process, and Redis stays unwired.**
A single gateway process is a coherent product, and everything the scheduler
needs -- slot counts, queues, virtual clocks -- is cheap and correct in memory.
Distributing it means Lua scripts for atomic counters, lease TTLs and a reaper
for slots held by a replica that died, and a new dependency in the request path,
all to solve a problem that does not exist until there is a second replica. The
seam is the `Scheduler` class: everything above it goes through `acquire()`.

**Tenants are configuration, not database rows.**
They are edited deliberately, reviewed, and deployed; there is no self-service
signup that would need to write them at runtime. Postgres would add a failure
domain and an async query on the request path in exchange for nothing the
scheduler uses.

**Capacity is a lease held by an async context manager.**
This is the whole defence against capacity leaks. Completion, provider failure,
client disconnect and unexpected exceptions all exit the block and all release.
A release path that must be remembered at each call site is one that eventually
is not. A leaked slot is permanent -- usable concurrency drops and never
recovers -- and it presents as a gradual slowdown that looks like a provider
problem, so it is worth making structurally impossible rather than merely
tested. Every exit path still gets its own test.

**There is no uncontended fast path.**
Even when capacity is free, a request is enqueued and dispatched by the same
pump that serves queued work. A separate fast path would be a second admission
mechanism with its own ordering, and it would let an arriving request overtake a
queued one whenever a slot happened to be free. The cost is one
already-resolved future per request.

**Fairness is weighted fair queueing on virtual clocks, not deficit round robin.**
Both give the same weighted share. Virtual time is simpler for one-at-a-time
dispatch -- serve whichever backlogged tenant's clock is furthest behind -- and
skipping a tenant that is at its own ceiling is just a filter on the minimum
search rather than a special case in a rotation. A returning tenant rejoins at
the current minimum so it cannot bank credit while idle and then drain the
gateway on return.

**Fairness is measured in tokens, and settled against actual consumption.**
A tenant sending 4000-token completions consumes roughly ten times the capacity
of one sending 400-token completions at the same request rate; request-count
fairness would call that fair. Scheduling has to commit before the cost is
known, so the virtual clock advances on a p50 estimate and is corrected by the
real figure when the lease is released. Without that correction a tenant whose
requests systematically run longer than predicted would be under-charged on
every one and drift into a permanently larger share. With it, the predictor's
accuracy affects how quickly the books balance, not whether they do.

**Two quantiles for two different jobs.**
The scheduler estimates cost at p50 -- an unbiased guess, later corrected. Budget
reservation will use p95, because over-reserving costs some idle capacity while
under-reserving overspends a tenant's budget. Output lengths are tracked as the
mean and variance of `log(tokens)`: the distribution is right-skewed, so an
arithmetic mean sits above the typical value and an arithmetic standard
deviation implies negative lengths.

**Reserved floors are enforced by splitting capacity, not by preemption.**
`shared_capacity = max_concurrency - sum(reserved)`. A tenant below its floor
always has a slot; above it, it competes for the shared pool. A tenant with no
reservation can therefore never occupy more than the shared pool holds, which is
what makes another tenant's floor reachable no matter how much load the first
one offers. Configuration refuses reserved totals above capacity, because
guarantees that cannot all be honoured at once are an overdraft, not a floor.

**Admission happens before the response body starts.**
For streaming requests the lease is acquired before the `StreamingResponse` is
constructed. Once the body has started the status line is already sent, and a
200 followed by an error frame is a much worse way to say "we are full" than a
429 with `Retry-After`.

**A configuration with no tenants runs in open mode.**
A fresh checkout serves requests without a setup step, using a synthetic
`default` tenant that is present in the scheduler but deliberately absent from
the auth registry, so it cannot be authenticated as. `/health` reports which
mode is active. Any configuration with tenants requires authentication.

**API keys are SHA-256 with a server-side pepper, not argon2.**
Password KDFs are slow on purpose, to make brute force expensive against
low-entropy secrets. A 128-bit random key has nothing to brute-force, and
spending ~50ms of CPU per request would make authentication the largest single
component of gateway latency. The pepper is what stops a leaked config file from
being enough to mint a working key.


## Week 2 -- budgets

**The reservation is the request's ceiling, not its predicted length.**
This is the one place the prediction is deliberately *not* used, and the reason
is that the two consumers have different failure costs. A scheduling
mis-estimate is self-correcting: settlement adjusts the tenant's virtual clock
and fairness converges anyway. A budget mis-estimate is tokens the provider has
already generated and billed for, and no later correction undoes that. So the
scheduler gets the p50 estimate and the ledger gets the ceiling. Because a
request can never emit more than its `max_tokens`, reserving exactly that makes
`spent + sum(reserved) <= limit` an invariant that holds by construction rather
than by probability.

**Clamping rather than refusing near the limit.**
Reserving the ceiling means a tenant with 500 tokens of headroom cannot start a
request declaring `max_tokens=4096`, even though it would probably have used
150. Refusing it would be needlessly strict. Instead `max_tokens` is reduced to
the remaining headroom and the client is told through a response header, so a
short answer is explained rather than mysterious. Below a small floor the
request is refused outright: serving with a handful of tokens of headroom
produces a stub that costs a provider call and helps nobody.

**Budget exhaustion is 402, not 429.**
A full queue is 429 with `Retry-After` because retrying will succeed. An
exhausted budget will never succeed on retry, and telling a client to retry
something that cannot work is worse than telling it nothing.

**The binding reservation is taken after the request has capacity, not before it queues.**
A cheap non-binding check runs first, so a request that clearly cannot be paid
for does not occupy a queue slot another tenant could have used. But holding a
real reservation through a long queue wait would reject sibling requests the
tenant could actually afford, since the reservation is the ceiling. So the
binding reservation happens once the scheduler has granted a slot.

**Capacity and budget are released by one exit path.**
Both live on an `AsyncExitStack` that the streaming generator owns, so
completion, provider failure, client disconnect and unexpected exceptions all
settle identically. An un-released budget reservation is as damaging as a leaked
capacity slot -- it permanently shrinks what the tenant can spend -- and it is
just as easy to forget.

**The ledger caps the charge at the reservation.**
Unreachable in normal operation, since the clamp is what the provider is told.
It exists so that a provider ignoring `max_tokens` becomes a capped charge
rather than an overspend: the ledger is the last place a provider bug could turn
into a billing one.

## Week 2 -- what the fairness benchmark showed

Run `make bench-fairness`. Three tenants, capacity well below offered load, the
same seed in both arms so arrivals and provider responses are identical and only
the scheduling decision differs.

Two distinct results, worth separating because a policy can do well on one and
badly on the other:

*Isolation.* The tenant asking for less than its share saw median queue wait
fall from 19541ms under FIFO to 82ms under weighted fair queueing, with
rejections going from 10 to zero. Under FIFO it was simply behind a flood it had
no part in.

*Proportionality.* With the light tenant taking 13.5% of tokens, the 86.5%
actually contended should divide 21.6% / 64.9% by weight; measured 19.8% /
66.7%. Jain's index over weight-normalised shares moved from 0.784 to 0.997.

A second run at a different seed over a longer window gave 0.999 and a
proportionality gap narrowing from +-1.8pp to +-1.1pp, which is convergence
rather than systematic bias. No scheduler change was needed.

The backlogged tenants still wait about 20 seconds and shed most of their load.
That is correct: they are offering roughly seven times what the gateway can
serve, and the bounded queue and deadline reject the excess instead of
accumulating work nobody is waiting for. Fair scheduling decides who waits. It
does not create capacity.

**A finite burst does not test fairness.** An early smoke test had two tenants
send equal finite bursts and finish with equal token counts under a 3:1 weight,
which looked like a bug and was not: if every tenant's work eventually drains,
all of it completes regardless of ordering. Weighting changes who waits, so the
benchmark has to keep the contended tenants backlogged for the whole measurement
window.


## Week 3 -- reliability

**Four timeouts, not one.**
A single read deadline cannot distinguish a provider that never answered from
one that answered and then stalled, and it makes both wait its full duration
before anything notices. Connect, time-to-first-token, inter-token and total now
each produce a distinct error class, because what happens next depends on which
one fired.

The per-chunk deadlines wrap only the await on the next chunk, never the yield
to the consumer. Otherwise backpressure from a slow client would be measured as
a stalled provider, and the breaker would open for something the provider did
correctly. The total deadline is wall-clock and therefore *does* include time
spent writing to the client, which is why it is excluded from provider health.

**Failover is phased on what has been delivered, not on the kind of error.**
Before the first token nothing has reached the client, so another provider can
serve the request and the caller never learns the first one failed. After the
first token it cannot: switching would splice a second continuation onto a
partial answer and return corrupted output under a success status. A visible
error can be retried by the caller; silently corrupt output cannot even be
detected. So a mid-stream failure terminates with a typed frame carrying how
many tokens actually arrived.

This is the reason the TTFT deadline is deliberately tight. It is the lever that
moves failure mass out of the unrecoverable window and into the one where
recovery is invisible, and `switchyard_terminal_failures_total` is labelled by
phase so the effect of moving it is measurable rather than assumed.

**Failover reuses one capacity lease and one budget reservation.**
A retry is still one request competing for capacity, so it holds one slot for
its whole life however many providers it tries. And because failover only
happens when zero tokens were produced, there is no output to double-charge.
Both are asserted under load rather than argued.

**Only failures the provider is responsible for count against it.**
A malformed request fails identically everywhere, so letting one tenant's bad
requests open the breaker would take a healthy provider away from every other
tenant. A total timeout can be caused by a slow consumer. `ErrorClass` carries
that distinction as `counts_against_provider`, so the policy lives with the
taxonomy rather than being re-derived at each call site.

**The breaker needs a minimum sample count, jitter, and bounded probes.**
Without a minimum, the first failure after a quiet period is a 100% failure rate
and opens the breaker on one unlucky request. Without jitter, everything waiting
on a provider retries at the same instant it half-opens, so a recovering
provider is hit by a synchronised stampede. Without a probe limit, the same
thing happens within the half-open window. The cooldown also doubles on repeated
trips, so a provider that is still broken is backed off rather than polled.

**An abandoned probe is released.**
A client disconnecting mid-stream leaves a half-open probe with no verdict. If
the slot were not returned, the breaker could never gather enough evidence to
close and the provider would stay shut out permanently -- a liveness bug that
only appears under exactly the conditions that make it hardest to notice.

**Exhaustion reports every attempt, not just the last error.**
When several providers are tried and all fail, returning only the final message
hides that a failover happened at all. "all providers for model X are
unavailable (a: server_error, b: connect)" is the difference between diagnosing
one bad provider and diagnosing an outage. This was found by a test that
expected the aggregate and got the raw last error.

**Drain treats queued and running work differently.**
A queued request has not started, so refusing it costs nothing and the client
can go elsewhere immediately. A running one has already consumed provider
capacity and may have delivered tokens, so killing it wastes what was paid for
and hands back a truncated answer. Queued work is therefore rejected at once and
running work is waited for.

The wait is bounded. A provider that never finishes would otherwise hold
shutdown open indefinitely, so past the timeout the remainder is abandoned and
*reported* rather than silently waited on. `/health` returns 503 while draining
-- readiness, not liveness: the process still works, it just should not be given
anything more.

**The router takes a per-request observer.**
Which provider served a request, and how many attempts it took, are facts about
that request rather than about the router. Passing the observer per call is also
what lets the pump label metrics with the provider that actually answered, which
is not known until one does and may not be the first one tried.


## Week 3 -- what the outage benchmark showed

Run `make bench-faults`. Steady load, one provider returning 5xx from 12s to 30s,
same workload and seed in both arms; the only difference is whether a fallback
exists.

Client-visible success during the outage went from 16/120 (13%) without failover
to 48/48 (100%) with it. The breaker opened at 16.3s in the single-provider arm
and 21.4s with failover, and 26 requests were served by the fallback.

Two things worth noting because they are easy to get wrong.

*Surviving an outage is not free.* With failover, p99 rose from about 3s to
10-20s: requests moved to a slower provider and capacity saturated. Reporting
only the error rate would make the reliability layer look costless.

*The breaker helps even with nowhere to fail over to.* In the single-provider arm
p99 **dropped** to about 10ms during the outage, because once the breaker opened
the gateway stopped paying to rediscover the outage on every request. Fast
failure is a genuine improvement over slow failure even when the answer is still
an error, and it is a separate benefit from failover.

**The benchmark found a bug in the instrument, not the product.** The first run
reported 370/370 requests completing during a total outage with no fallback. The
load generator judged success by stream *shape*: any well-formed SSE stream
ending in `[DONE]` counted as complete. But a typed error frame ends exactly that
way -- carrying the failure in the terminal frame is the whole design -- so every
provider outage scored as a success. The generator now reads `finish_reason`, and
a regression test asserts that a stream can be well-formed and still be a failed
request.

That also surfaced a product wart. A non-streaming request where every provider
failed returned 200 with an error body, making the client inspect the body to
discover the failure. It now returns 502 when nothing was delivered. Partial
content still returns 200: the client has real tokens, and the error frame
explains why there are not more -- the same contract the streaming path offers.

**Not every number here reproduces, and the difference matters.** The
during-outage figures, the failover count and the moment the breaker first opens
are stable across runs. Whole-run completion for the single-provider arm is not:
two runs of the identical scenario gave 58% and 24%. Whether a half-open probe
lands while the provider is still broken decides how long the breaker stays shut
afterwards, and the jittered doubling cooldown moves recovery by tens of seconds.
That is the backoff working as designed rather than measurement noise, so the
documented result is the stable one.


## Week 4 -- deployment and productisation

**The configuration is mounted into the container, not baked into the image.**
This was found by finally running the stack rather than validating it. The
Dockerfile copied `src/` and `pyproject.toml` but never the configuration, so
the gateway container had been crashing on startup for three weeks behind a
compose file that validated cleanly. Mounting is also the better answer on its
own terms: configuration carries credentials, and changing a limit should not
require a rebuild.

The lesson is narrower than "test your deployment": `docker compose config`
validates syntax and says nothing about whether the thing starts. A healthcheck
on the gateway service now makes a failure to start visible instead of silent.

**Scenarios stop their flood rather than waiting for it to drain.**
The noisy-neighbour scenario awaited every flooding request, which meant sitting
through the tenant's deadline for a full queue -- an intermittent 26-second
scenario that took 86. Those requests exist to be rejected; waiting for them
measures the deadline, not the scheduler. The same fix had already been made in
`verify` and was simply never carried back, which is its own lesson about
fixing a pattern in one place.

**A demonstration command never ends in a traceback.**
The same investigation surfaced a scenario crashing with `httpx.ReadTimeout`
because its closing status read had a five-second timeout while the gateway was
still finishing work. A failed status read now degrades to "could not confirm"
and the check is reported as skipped. Whatever else is true, a command whose
purpose is to show the product working must not fall over while doing it.

**The flooding tenant gets a short deadline.**
At 25 req/s with a ten-second deadline the load generator holds several hundred
open connections, and both generators share the scenario runner's event loop --
enough to slow the very measurement being taken. A three-second deadline is
realistic configuration for a tenant you expect to shed, and it keeps the
instrument from contending with itself.

**The demo is a command and a transcript, not a video.**
`make demo` runs contention, then an outage, then a configuration check. A
recorded run lives in `docs/demo.md` for people who will not clone the
repository. No terminal-recording tooling is installed here, and a GIF would
have meant adding a dependency to produce an artifact the product already
communicates on its own.

**Deployment is one process, and Compose is the supported path.**
Verified end to end: build, healthy start, authenticated Prometheus scrape,
provisioned Grafana dashboard with every panel returning data. Nothing in the
product needs an orchestrator, and a single replica is the honest scope for a
scheduler whose state is correct in memory.

## Week 4 -- durable spend

### A budget bounded by process uptime is not a budget

Everything else in the gateway is safely ephemeral. Queues drain, leases
expire, the output-length predictor re-converges in a few hundred requests,
and a circuit breaker re-learns provider health within seconds of restarting.
Spend does not re-learn. A tenant that had consumed 95% of its allowance
before a deploy came back at zero.

That made the hard bound the budget ledger works to maintain -- reservation at
the ceiling, `max_tokens` clamped to remaining headroom, so overrun is
impossible by construction -- bounded in practice by how often the process
restarted. The invariant was real and the guarantee was not.

So `spent` is written to a file and read back at startup. Nothing else is:
`reserved` covers in-flight requests, and after a restart nothing is in
flight, so persisting it would permanently leak headroom from every tenant
that was busy when the gateway went down.

### A JSON file, not a database

The dataset is one integer per tenant. Tens of rows, a few hundred bytes. A
database would contribute a schema, a migration path and a failure domain to
hold less data than this section.

The file is written whole to a temporary file in the same directory, fsynced,
and moved into place with `os.replace`. A crash mid-write therefore leaves the
previous version intact rather than a truncated one -- a torn state file is
worse than a stale one, because the stale one still parses. Being plain text
also matters more than it sounds: an operator asking what a tenant has
actually spent can answer it with `cat`.

### The durability bound is stated, and write-through is refused

Flushing on every settle was the obvious first design and is wrong. It puts an
fsync on the completion path of every request, which would make a gateway
whose entire purpose is scheduling throughput wait on a disk several hundred
times a second.

Instead writes are batched on an interval, with one more flush after drain so
that requests finishing during shutdown are included. The cost is a stated
bound: a crash can lose up to `state_flush_interval_s` of recorded spend. The
direction of that error matters and is worth being explicit about -- a tenant
is briefly **under**-charged for requests that completed inside the last
window, never over-charged. Tightening it means shortening the interval, not
making the write synchronous.

`state_flush_interval_s = 0` is rejected by config validation rather than
silently accepted, because the configuration that asks for it is asking for
the behaviour that was deliberately not built.

### Refusing to start beats starting wrong

A state file that cannot be parsed, carries a version this build does not
write, or contains a negative spend raises at app-factory time -- before the
port is bound. The alternative is a gateway that reports itself healthy while
serving every tenant a fresh budget, which is the exact failure the file
exists to prevent, arriving silently.

A tenant in the file but absent from the configuration is ignored rather than
dropped: its total is merged back in on the next write, so removing a tenant
for a day does not quietly refund it.

### Found while testing it

Writing the failure-path tests surfaced that `mkdir` sat outside the `try`
block in the save path, so a state file whose parent directory could not be
created raised a bare `OSError` while every other write failure raised
`StateError`. Callers catching the documented exception would have missed it.
Moving one line fixed it; the test that found it is
`test_existing_file_survives_a_failed_write`.

The static review also gained a `budgets survive a restart` check. It reports
`SKIP` rather than `FAIL` when no state file is configured, because running
without one is a legitimate choice for a demo -- but it should be a chosen one,
not a discovered one.

## M0 -- why CI was red for two months and what changed

### The symptom

Every push from 28 August onward failed. The only green run was the first one.
`ruff` and `mypy` passed every time; `pytest -q` exited with code 2 after about
four seconds. The same commit, in a clean clone, passed locally on both 3.13
and 3.14. That combination is what made it easy to ignore: the failure looked
like a runner problem, and the tests "obviously" worked.

### The cause

`tests/test_reliability.py` imports fixtures at module scope:

```python
from tests.conftest import build_tenant_config, serve_gateway
```

Ten other call sites do the same inside functions. All of them need the
repository root on `sys.path`, because `tests` is only importable as a package
from there.

`python -m pytest` puts it there -- running a module with `-m` prepends the
working directory to `sys.path`. The `pytest` console script does not. The
Makefile uses `$(PY) -m pytest`; CI ran `pytest -q`. One line of difference,
and it decided whether the suite could import itself.

Exit code 2 is pytest's `INTERRUPTED`, raised when collection fails -- not a
failing assertion. Four seconds was import time. Because collection aborted at
the first module-scope import, **the suite never ran at all on CI**: the other
ten call sites had not even been reached.

### The fix, and why it is this one

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
```

The real defect was not the invocation, it was depending on a side effect of
it. Changing CI to `python -m pytest` would have turned the job green while
leaving the suite just as fragile -- the next person to run `pytest` would hit
the same wall. Declaring the path requirement in configuration makes the suite
run identically however it is launched, which is the property that was missing.

Verified both ways: `pytest -q` and `python -m pytest -q` each report 356
passed.

### What else changed, so this class of failure is visible sooner

- `runs-on: ubuntu-24.04` instead of `ubuntu-latest`. The hosted image began
  migrating to Ubuntu 26 on 19 October; an unpinned runner changes with no
  commit to point at. That was a second outage waiting to happen.
- An `Environment` step printing `python -VV`, `uname -a` and `pip freeze`, so
  a future red run is diagnosable from its own log rather than by reproducing
  it by hand.
- A separate `pytest --collect-only -q` step. `ruff` and `mypy` never import
  the package, so an import error can only surface at collection; giving it its
  own step stops "the suite could not load" and "a test failed" from rendering
  as the same red X.
- Every action pinned to a commit SHA with its version in a comment, and
  upgraded off the deprecated Node 20 runtimes (`checkout` v7, `setup-python`
  v7, `upload-artifact` v7, `download-artifact` v8).

### The same mistake, a second time

Fixing the test job let the `bench` job run for the first time, and it failed
immediately: `make: .venv/bin/python: No such file or directory`. The Makefile
hardcoded `PY := $(VENV)/bin/python`, but CI installs with `pip install -e .`
into the runner's own Python and has no virtualenv. Because that job only runs
on manual dispatch, nobody had ever seen it.

It is the same defect as the pytest one wearing different clothes: the build
assumed a developer's machine. `PY` and the tool paths now fall back to what is
on `PATH` when `.venv/bin/python` is absent, so the targets work in both
places. `install` keeps the explicit venv path, because the conditional is
evaluated when the Makefile is parsed -- before the venv it creates exists.

### The part worth remembering

A green local suite and a red CI suite are not a contradiction to be explained
away; they are two different environments disagreeing, and the disagreement is
the information. The gap here was one word -- `-m` -- and it hid the fact that
no test had executed in CI for two months.

## M1 -- liveness, readiness, and build identity

### Why /livez and /readyz are different endpoints

One health endpoint has to answer two questions that want opposite actions.
A failed *readiness* check should stop traffic arriving. A failed *liveness*
check should restart the container. Most of the time both answers agree, so
one endpoint seems sufficient -- until the gateway drains.

Draining is exactly the state where they disagree. The gateway is deliberately
refusing new work while it finishes the streams it already holds. Readiness
must fail, so the load balancer stops sending. Liveness must hold, because
restarting then would kill the in-flight requests the drain exists to protect
-- the 30-second drain would become a 30-second window in which any restart
destroys work.

So `/livez` returns 200 always, and deliberately reads no scheduler state: a
liveness probe that a busy or wedged subsystem can fail will restart a process
that was about to recover. `/readyz` keeps the old behaviour, 503 while
draining. `/health` stays as an alias of `/readyz` because that is what every
existing caller already uses. The Dockerfile's HEALTHCHECK asks `/livez`;
Compose's `depends_on: service_healthy` asks `/readyz`, because "has it
started" and "can it take traffic" are different gates.

### Build identity on the readiness endpoint

A deploy that cannot answer "which commit is actually serving?" asks to be
trusted. `/readyz` now carries `version`, `git_sha`, `started_at` and
`uptime_s`, and there is a `switchyard_build_info{version,git_sha}` gauge
fixed at 1 -- the conventional info-metric shape, so a dashboard can join on
it and answer "did errors rise when the sha changed?".

The sha comes from `SWITCHYARD_GIT_SHA`, set from a Docker build argument.
Outside a built image it reads `dev` rather than inventing a commit: "not
built by the pipeline" is the true answer for a laptop run. Uptime is measured
with `time.monotonic()`, because the wall clock can step under an NTP
correction or a suspended laptop and produce an uptime that goes backwards.

Putting it on readiness rather than a new endpoint is deliberate: the deploy
smoke test already calls `/readyz`, so it can confirm the intended SHA is the
one answering in the same request that proves the gateway is up. A rollback is
confirmed the same way.

### Provider kind, without leaking the endpoint

`/v1/providers` now reports `kind` as `synthetic` or `external`. The console
turns this into its Simulation banner, so it is derived from configuration
rather than a flag someone can forget to flip. A provider explicitly declared
under `[providers.*]` reports `external` even when its base URL happens to
point at the synthetic fleet, which is the honest answer: an operator has
taken responsibility for where that traffic goes.

It reports only the kind. The base URL and the name of the environment
variable holding the key are both things an admin endpoint should not hand
out, and a test asserts neither appears in the response.

### stop_grace_period, or why the drain was being cut short

`drain_timeout_s` is 30 seconds. Docker sends SIGTERM, waits **10** seconds by
default, then SIGKILLs. Every drain was therefore being cut off at a third of
its budget, killing in-flight streams the gateway was still finishing -- the
graceful shutdown worked perfectly and was then shot. `stop_grace_period: 40s`
gives the drain its full 30 seconds plus headroom for the final spend flush
that follows it.

### The base image is pinned by digest

`python:3.14-slim` moves. A base image that changes underneath you makes a
reproducible build a coincidence, and it is the layer with the most CVEs and
the least visibility. Pinned to the multi-arch index digest, so linux/amd64
(the EC2 host) and linux/arm64 (an Apple Silicon laptop) both still resolve
from the one pin. The readable tag stays in a comment.

### A timing test that was measuring the host

Adding sixteen tests made `test_generator_health_reflects_lag_relative_to_the
_workload` fail -- not because anything broke, but because it asserts the load
generator could fire on schedule, and a machine busy from 370 preceding tests
cannot. Measured: p99 scheduling lag is 1.37ms (0.66% of request latency) when
that module runs alone and 15.75ms (7.54%) behind the full suite, against a 5%
threshold. The flag was telling the truth; the assertion was about the host.

Marked `slow`, matching the reasoning CI already applies to the benchmarks --
meaningful latency numbers need a quiet machine. `make test-all` then still
failed, because it ran the slow tests behind everything else in the same
process. Splitting it into two pytest invocations fixed that: a fresh
interpreter restores the conditions a timing measurement needs. No assertion
was weakened.

## M2 -- the console backend

### Why a second app rather than routes on the gateway

The console holds the admin credential, polls four services once a second and
can generate load. None of that belongs in the request path of a gateway whose
entire argument is careful control of scarce capacity. A separate ASGI app in
the same package and the same image keeps them apart at runtime while keeping
one thing to build, version and deploy.

It also makes the trust boundary obvious: the gateway's admin endpoints stay
admin-only, and the console is the one process holding the key. The browser
never receives a credential -- the snapshot it renders is derived numbers.

### Counters in, rates out, and the two traps

The gateway exposes monotonic totals. A live view needs the opposite, so the
collector keeps the previous scrape and divides the difference by elapsed
time. Two things make that harder than it sounds.

**Counters reset.** A gateway restart sends every total to zero. Subtracting
naively gives a large negative number, which renders as a spike in the wrong
direction. A value below the previous one can only mean a restart, so the
delta is the new value itself -- slightly understating the first interval
after a restart, which is the right direction to be wrong in.

**Percentiles do not average.** A p95 over the last minute is not the mean of
per-second p95s. It is recomputed from the difference in cumulative histogram
buckets across the window, interpolating inside whichever bucket holds the
95th observation. The resolution is bounded by the bucket edges, so the UI
labels it approximate rather than implying precision it does not have. When
nothing was observed, it reports nothing -- zero would read as "very fast"
instead of "no data".

### Events are derived, not emitted

Nothing in the gateway tells the console that a breaker opened. The collector
notices a value changed between two snapshots and writes the sentence. That
keeps console concerns out of the gateway, and lets the feed say what a change
*meant* -- "breaker for 'fast' opened; traffic now goes to the next provider"
-- rather than echoing a state name.

### A demo that is allowed to fail

The guided scenarios score themselves from the same snapshots the live map
draws. Checks start `pending`, not `pass`: a demo showing green before
anything has happened teaches the audience to ignore it. If the gateway
genuinely fails to protect the quiet tenant, the console says FAIL in front of
everyone. A demo that cannot fail is an animation.

### Caps, because this runs on a t3.micro

Total offered load is capped at 30 rps, open streams at 120, and every profile
auto-stops after 120 seconds. The caps are enforced in `Profile.validate()`
and tested, not written in a comment and hoped for. Over the cap, arrivals are
*dropped and counted* rather than queued: queueing would quietly turn the
generator closed-loop, which is the measurement error the open-loop design
exists to avoid, and the count lets the console say the generator was the
limit rather than the gateway.

### Four bugs found by running the demo end to end

The outage scenario failed its `recovered` check, which looked like a flaky
test. It was not.

1. **`clear()` was a no-op.** The fleet's control endpoint applies a *patch*,
   and `{"faults": {}}` is an empty patch. The provider stayed broken after
   the "recovery", every breaker probe failed, and the cooldown doubled to 34
   seconds. Clearing now sets each fault rate to zero explicitly.
2. **`error_class` was read as a fault.** It is always populated -- it says
   what kind of error *would* be returned -- so filtering faults by
   truthiness painted every healthy provider as faulty. Only the three rates
   (`error_rate`, `abort_rate`, `stall_rate`) mean something is wrong.
3. **The profiles response was misread.** It wraps the provider map beside the
   run seed; the collector indexed the envelope and found nothing, so injected
   faults never appeared in the UI at all.
4. **`{"stall": true}` would have raised.** There is no boolean `stall` field;
   there is a `stall_rate`, because a provider stalling one request in ten is
   a different failure from one stalling every time. The toggle means
   "always", and the console is what knows that.

All four are regression-tested, including one test that deliberately pins the
fleet's patch semantics so a future change to replace-semantics is caught
rather than silently fixing and then re-breaking this.

### The recovery phase waits instead of guessing

The breaker's cooldown doubles on each failed probe, so how long recovery
takes depends on how long the outage lasted -- after 25 seconds of total
failure it had backed off past any fixed window. Rather than weaken the check
or pretend the backoff is not there, the recovery phase ends as soon as the
breaker closes, with a generous bound. The outage is 15 seconds for the same
reason: long enough to show the breaker open and traffic move, short enough
that the recovery half of the story still arrives.

Related: the steady traffic profile now runs longer than the scenario. A
half-open breaker closes by succeeding on a probe, and a probe needs a
request -- if the generator stops first, the breaker can never close and the
demo ends on a failure that is an artefact of the generator.

### The session cookie cannot be Secure

The free-tier deployment is plain HTTP: no load balancer, no certificate, no
budget for either. A `Secure` cookie would never be sent, so it is not set.
That is a real weakness, recorded rather than hidden -- anyone on the path can
read the cookie and drive the demo controls. It guards buttons that generate
synthetic load against a synthetic fleet, which is the only reason it is
tolerable. HTTPS is the first thing to buy.

## M3 -- the live map

### No framework, and what that bought

Three files the browser loads directly: an HTML shell, a stylesheet, and two
ES modules. No bundler, no dependency tree, no build step between the source
and what runs. For a UI this size that is not asceticism -- it is one fewer
toolchain to install, pin, audit and explain, and the thing deployed is the
thing in the repository.

The cost is manual DOM work. The benefit showed up immediately in the CSP: the
page loads nothing from anywhere else, so the policy can be
`default-src 'self'` with no `unsafe-inline`, and a test asserts the page has
no inline styles, no inline scripts and no event-handler attributes. Those are
invisible failures otherwise -- a browser silently refuses to apply the style
and the page simply looks wrong.

### A particle is not a request

Dots travel tenant to gateway to provider at a rate proportional to measured
throughput. One dot is roughly one request per second, not one request. At 25
rps the honest version would be unreadable and at 300 it would be a solid bar,
so the map draws a rate and the legend says exactly that. Animating one dot
per request would be a more impressive lie.

Motion is decoration throughout: every number the particles encode is also
present as text, which is why `prefers-reduced-motion` can switch the flow
layer off and lose nothing. Rendering is capped at 30fps and pauses when the
tab is hidden, because this shares a CPU with the gateway it is measuring.

### Capacity drawn as slots, not a percentage

The gateway panel draws `max_concurrency` individual squares, each outlined in
the colour of the tenant whose reserved floor owns it, dashed where it belongs
to the shared pool, and filled when occupied. A utilisation bar would say
"58%"; the grid shows *whose* 58% and which slots nobody else can take. That
is the difference between a number and an explanation, and the floor stops
being a line in a config file.

### Three bugs that only running it could find

1. **Signing in worked, then undid itself a second later.** `EventSource` was
   opened before the session cookie existed, so every snapshot on that stream
   reported `authenticated: false` and overwrote the local flag. The fix is to
   reconnect the stream after login; the stream is also now trusted to grant
   access and never to revoke it mid-connection.
2. **Buttons were rebuilt sixty times a minute.** Every snapshot replaced the
   whole control panel, so a click could land on a node that had just been
   swapped out -- which is exactly what happened while testing. They are now
   rebuilt only when their own state changes.
3. **Two strings shared one baseline.** At 420px the tenant card's served and
   queued lines fit side by side; at 236px -- the width that actually ships --
   they overlapped. Fixed by giving each its own line, which is also what the
   phone layout wanted.

None of these would have been caught by the API tests, because all three live
between a browser and a server that were each behaving correctly.

### A check that reported FAIL when it meant "no evidence"

The noisy-neighbour demo scored its third check -- that refusals land on the
tenant causing them -- as a failure, with the detail "the flood never exceeded
capacity". That detail was true and the verdict was wrong: with 24 slots and a
256-deep queue, acme's flood queued instead of being refused, which is the
gateway working. There were no refusals to attribute.

It now distinguishes three cases: the quiet tenant being refused is a genuine
FAIL, only the noisy tenant being refused is a PASS, and nothing refused
anywhere leaves the check pending with what to change to exercise that path.
Marking a correctly-behaving gateway broken is the worse error, and a demo
that cries wolf teaches its audience to stop reading the verdicts.

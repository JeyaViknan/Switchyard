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

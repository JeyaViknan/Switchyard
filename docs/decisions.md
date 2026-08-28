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

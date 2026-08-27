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

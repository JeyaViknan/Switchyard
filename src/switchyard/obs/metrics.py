"""Prometheus metrics and the request timing model.

Timing model
------------
One request is decomposed into disjoint spans that sum exactly to the latency
the client observes. `gateway_overhead` is defined as the *residual*, so the
decomposition can never silently fail to add up:

    arrived_at ......... request accepted by the HTTP layer
      |  queue_wait ..... blocked in admission control (recorded by the queue)
      |  gateway work ... parse, validate, route, cache lookup, serialization
    dispatched_at ...... provider call initiated
      |  provider_time .. dispatch -> terminal event from the provider
    provider_done_at
      |  gateway work ... final frame serialization, teardown
    completed_at ....... response fully written to the client

    total            = completed_at - arrived_at
    provider_time    = provider_done_at - dispatched_at
    queue_wait       = recorded explicitly by the admission controller
    gateway_overhead = total - queue_wait - provider_time

What `gateway_overhead` does and does not include
-------------------------------------------------
It includes everything Switchyard does before dispatching and after the
provider finishes. It does **not** include per-chunk pump work that happens
while the provider stream is open, because that work is interleaved with
provider waits inside `provider_time` and separating the two would require
timestamping around every chunk -- a measurement whose cost is comparable to
the quantity being measured. `event_loop_lag` is the metric that surfaces pump
CPU cost; if pump processing becomes expensive, it shows up there.

`queue_wait` is a value the admission controller records, not a difference
between two marks. Deriving it from timestamps would let ordinary parse and
route time drift into it, and would make it silently zero if a future code path
forgot to set a mark. As a value with an explicit owner, an unset queue wait is
zero because nothing queued -- which is the truth in week 1.

Cardinality
-----------
Every label is bounded: provider and model come from configuration, outcome is a
closed set. Request id is never a label. One unbounded label is enough to make a
metrics backend unusable.

Buckets
-------
LLM latencies span three orders of magnitude. Prometheus defaults top out at 10s
and would put most generations in `+Inf`, making p99 unmeasurable. Latency and
overhead use separate bucket sets because they differ by ~1000x.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

LATENCY_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0, float("inf"),
)

OVERHEAD_BUCKETS = (
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
    0.025, 0.05, 0.1, 0.25, 1.0, float("inf"),
)

REGISTRY = CollectorRegistry()

REQUESTS = Counter(
    "switchyard_requests_total", "Requests by outcome.",
    ["model", "provider", "outcome"], registry=REGISTRY,
)

REQUEST_DURATION = Histogram(
    "switchyard_request_duration_seconds",
    "Total client-observed latency: request arrival to response fully written.",
    ["model", "provider"], buckets=LATENCY_BUCKETS, registry=REGISTRY,
)

TTFT = Histogram(
    "switchyard_ttft_seconds",
    "Arrival to first content token, as the client sees it. Includes queue wait, "
    "deliberately: a queued request is slow to first token and the metric should "
    "say so.",
    ["model", "provider"], buckets=LATENCY_BUCKETS, registry=REGISTRY,
)

QUEUE_WAIT = Histogram(
    "switchyard_queue_wait_seconds",
    "Time blocked in admission control before capacity was granted.",
    ["tenant"], buckets=LATENCY_BUCKETS, registry=REGISTRY,
)

QUEUE_DEPTH = Gauge(
    "switchyard_queue_depth", "Requests waiting for capacity.",
    ["tenant"], registry=REGISTRY,
)

TENANT_INFLIGHT = Gauge(
    "switchyard_tenant_inflight", "Requests holding a capacity slot.",
    ["tenant"], registry=REGISTRY,
)

CAPACITY_UTILISATION = Gauge(
    "switchyard_capacity_utilisation",
    "In-flight requests as a fraction of gateway max_concurrency.",
    registry=REGISTRY,
)

ADMISSION_REJECTED = Counter(
    "switchyard_admission_rejected_total",
    "Requests refused before consuming provider capacity, by reason. Which "
    "limit is binding is the first question under overload.",
    ["tenant", "reason"], registry=REGISTRY,
)

DISPATCHED = Counter(
    "switchyard_dispatched_total", "Requests granted capacity.",
    ["tenant"], registry=REGISTRY,
)

TENANT_TOKENS = Counter(
    "switchyard_tenant_tokens_total",
    "Output tokens consumed per tenant. The unit fairness is measured in, so "
    "this is what a fairness panel should divide.",
    ["tenant"], registry=REGISTRY,
)

BUDGET_REMAINING = Gauge(
    "switchyard_budget_tokens_remaining",
    "Tokens a tenant may still spend: limit minus settled spend minus what is "
    "currently reserved for in-flight requests.",
    ["tenant"], registry=REGISTRY,
)

BUDGET_RESERVED = Gauge(
    "switchyard_budget_tokens_reserved",
    "Tokens held for in-flight requests. Held at each request's ceiling, not at "
    "its predicted length, so the spending bound holds by construction.",
    ["tenant"], registry=REGISTRY,
)

BUDGET_SPENT = Counter(
    "switchyard_budget_tokens_spent_total",
    "Tokens actually consumed and charged.",
    ["tenant"], registry=REGISTRY,
)

MAX_TOKENS_CLAMPED = Counter(
    "switchyard_max_tokens_clamped_total",
    "Requests whose max_tokens was reduced to fit remaining budget. A rising "
    "count means a tenant is running out, not that anything is broken.",
    ["tenant"], registry=REGISTRY,
)

PREDICTION_ERROR = Histogram(
    "switchyard_output_prediction_ratio",
    "Actual output tokens divided by the scheduler's estimate. Centred on 1.0 "
    "means the predictor is unbiased; drift shows scheduling decisions are "
    "being made on the wrong cost.",
    buckets=(0.1, 0.25, 0.5, 0.75, 0.9, 1.0, 1.1, 1.5, 2.0, 4.0, 10.0, float("inf")),
    registry=REGISTRY,
)

PROVIDER_TIME = Histogram(
    "switchyard_provider_time_seconds",
    "Dispatch to terminal event from the provider.",
    ["model", "provider"], buckets=LATENCY_BUCKETS, registry=REGISTRY,
)

GATEWAY_OVERHEAD = Histogram(
    "switchyard_gateway_overhead_seconds",
    "Latency attributable to Switchyard: total minus queue wait minus provider "
    "time. Excludes per-chunk pump work interleaved with provider waits; see "
    "event_loop_lag for that.",
    buckets=OVERHEAD_BUCKETS, registry=REGISTRY,
)

INTER_TOKEN = Histogram(
    "switchyard_inter_token_latency_seconds",
    "Gap between consecutive output tokens.",
    ["provider"], buckets=OVERHEAD_BUCKETS, registry=REGISTRY,
)

TOKENS = Counter(
    "switchyard_tokens_total", "Tokens processed.",
    ["model", "provider", "direction"], registry=REGISTRY,
)

INFLIGHT = Gauge(
    "switchyard_inflight", "Requests currently in flight to a provider.",
    ["provider"], registry=REGISTRY,
)

EVENT_LOOP_LAG = Histogram(
    "switchyard_event_loop_lag_seconds",
    "How late the event loop runs scheduled callbacks. In an asyncio gateway "
    "this is the dominant source of tail latency that is not the provider's "
    "fault: CPU-bound work on the loop shows up here first.",
    buckets=OVERHEAD_BUCKETS, registry=REGISTRY,
)


async def monitor_event_loop_lag(interval_s: float = 0.1) -> None:
    """Continuously sample event-loop delay by measuring sleep overshoot."""
    while True:
        start = time.perf_counter()
        await asyncio.sleep(interval_s)
        EVENT_LOOP_LAG.observe(max(0.0, time.perf_counter() - start - interval_s))


class RequestTimeline:
    """Phase marks for one request. See the module docstring for the model.

    Created at request arrival, before parsing or routing, so that every span is
    measured from the moment the client's request landed rather than from
    somewhere further down the pipeline.
    """

    __slots__ = (
        "arrived_at", "queue_wait_s", "dispatched_at", "first_token_at",
        "last_token_at", "provider_done_at", "tokens", "tenant_id",
    )

    def __init__(self, tenant_id: str = "-") -> None:
        self.tenant_id = tenant_id
        self.arrived_at = time.perf_counter()
        self.queue_wait_s = 0.0
        self.dispatched_at: float | None = None
        self.first_token_at: float | None = None
        self.last_token_at: float | None = None
        self.provider_done_at: float | None = None
        self.tokens = 0

    def record_queue_wait(self, seconds: float) -> None:
        """Called by the admission controller when a request is dequeued."""
        self.queue_wait_s = seconds

    def mark_dispatched(self) -> None:
        self.dispatched_at = time.perf_counter()

    def on_token(self, provider: str) -> None:
        now = time.perf_counter()
        if self.first_token_at is None:
            self.first_token_at = now
        elif self.last_token_at is not None:
            INTER_TOKEN.labels(provider=provider).observe(now - self.last_token_at)
        self.last_token_at = now
        self.tokens += 1

    def mark_provider_done(self) -> None:
        self.provider_done_at = time.perf_counter()

    def finish(self, model: str, provider: str, outcome: str) -> None:
        completed_at = time.perf_counter()
        total = completed_at - self.arrived_at

        # If the provider was never dispatched (rejected at validation) its span
        # is zero and the whole request is gateway time, which is correct.
        if self.dispatched_at is not None and self.provider_done_at is not None:
            provider_time = self.provider_done_at - self.dispatched_at
        else:
            provider_time = 0.0

        REQUESTS.labels(model=model, provider=provider, outcome=outcome).inc()
        REQUEST_DURATION.labels(model=model, provider=provider).observe(total)
        QUEUE_WAIT.labels(tenant=self.tenant_id).observe(self.queue_wait_s)
        PROVIDER_TIME.labels(model=model, provider=provider).observe(provider_time)
        if self.first_token_at is not None:
            TTFT.labels(model=model, provider=provider).observe(
                self.first_token_at - self.arrived_at
            )
        # Residual by construction, so the decomposition always sums to total.
        # Clamped at zero: clock granularity can make the parts marginally
        # exceed the whole on very short requests.
        GATEWAY_OVERHEAD.observe(max(0.0, total - self.queue_wait_s - provider_time))


@asynccontextmanager
async def track_inflight(provider: str) -> AsyncIterator[None]:
    INFLIGHT.labels(provider=provider).inc()
    try:
        yield
    finally:
        INFLIGHT.labels(provider=provider).dec()

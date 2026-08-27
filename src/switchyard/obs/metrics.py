"""Prometheus metrics.

Cardinality discipline
----------------------
Every label here is bounded: provider and model come from configuration, outcome
and reason are closed enums, and tenant is bounded by the tenant table. Request
id is never a label -- one unbounded label is enough to make a metrics backend
unusable, and it is the single most common way monitoring breaks.

Bucket choice
-------------
LLM latencies span three orders of magnitude, from a sub-millisecond cache hit to
a minute-long generation. The default Prometheus buckets top out at 10s and would
put most real generations in `+Inf`, making p99 unmeasurable. These buckets are
explicit and log-spaced across the range that actually occurs.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# Latency buckets, seconds. Dense below 1s (where gateway overhead lives) and
# sparse above (where generation time lives).
LATENCY_BUCKETS = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.0, 4.0, 8.0, 15.0, 30.0, 60.0, float("inf"),
)

# Gateway overhead should be sub-millisecond to low-milliseconds. Reusing the
# latency buckets would put essentially everything in the first bucket.
OVERHEAD_BUCKETS = (
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
    0.025, 0.05, 0.1, 0.25, 1.0, float("inf"),
)

REGISTRY = CollectorRegistry()

REQUESTS = Counter(
    "switchyard_requests_total",
    "Requests by outcome.",
    ["model", "provider", "outcome"],
    registry=REGISTRY,
)

REQUEST_DURATION = Histogram(
    "switchyard_request_duration_seconds",
    "End-to-end request duration as seen by the gateway.",
    ["model", "provider"],
    buckets=LATENCY_BUCKETS,
    registry=REGISTRY,
)

TTFT = Histogram(
    "switchyard_ttft_seconds",
    "Time to first token. The latency metric that matters for streaming clients.",
    ["model", "provider"],
    buckets=LATENCY_BUCKETS,
    registry=REGISTRY,
)

INTER_TOKEN = Histogram(
    "switchyard_inter_token_latency_seconds",
    "Gap between consecutive output tokens.",
    ["provider"],
    buckets=OVERHEAD_BUCKETS,
    registry=REGISTRY,
)

GATEWAY_OVERHEAD = Histogram(
    "switchyard_gateway_overhead_seconds",
    "End-to-end duration minus time spent waiting on the provider. This is the "
    "latency Switchyard itself is responsible for.",
    buckets=OVERHEAD_BUCKETS,
    registry=REGISTRY,
)

TOKENS = Counter(
    "switchyard_tokens_total",
    "Tokens processed.",
    ["model", "provider", "direction"],
    registry=REGISTRY,
)

INFLIGHT = Gauge(
    "switchyard_inflight",
    "Requests currently in flight to a provider.",
    ["provider"],
    registry=REGISTRY,
)

EVENT_LOOP_LAG = Histogram(
    "switchyard_event_loop_lag_seconds",
    "How late the event loop is running scheduled callbacks. In an asyncio "
    "gateway this is the dominant source of tail latency that is not the "
    "provider's fault: any CPU-bound work on the loop shows up here first.",
    buckets=OVERHEAD_BUCKETS,
    registry=REGISTRY,
)


async def monitor_event_loop_lag(interval_s: float = 0.1) -> None:
    """Continuously sample event-loop delay.

    Sleeps for a known interval and records the overshoot. A healthy loop
    overshoots by microseconds; a loop blocked by CPU work overshoots by however
    long that work took, which is exactly what needs to be visible.
    """
    while True:
        start = time.perf_counter()
        await asyncio.sleep(interval_s)
        EVENT_LOOP_LAG.observe(max(0.0, time.perf_counter() - start - interval_s))


class StreamTimer:
    """Records the timing of one streamed response.

    Tracks provider wait time separately from total time so gateway overhead can
    be reported as a first-class number instead of being inferred.
    """

    __slots__ = ("started", "first_token_at", "last_token_at", "provider_wait", "tokens")

    def __init__(self) -> None:
        self.started = time.perf_counter()
        self.first_token_at: float | None = None
        self.last_token_at: float | None = None
        self.provider_wait = 0.0
        self.tokens = 0

    def on_token(self, provider: str) -> None:
        now = time.perf_counter()
        if self.first_token_at is None:
            self.first_token_at = now
        elif self.last_token_at is not None:
            INTER_TOKEN.labels(provider=provider).observe(now - self.last_token_at)
        self.last_token_at = now
        self.tokens += 1

    def finish(self, model: str, provider: str, outcome: str) -> None:
        total = time.perf_counter() - self.started
        REQUESTS.labels(model=model, provider=provider, outcome=outcome).inc()
        REQUEST_DURATION.labels(model=model, provider=provider).observe(total)
        if self.first_token_at is not None:
            TTFT.labels(model=model, provider=provider).observe(self.first_token_at - self.started)
        # Provider wait is everything from dispatch to the last token; whatever
        # remains is time Switchyard spent on its own work.
        provider_time = (
            (self.last_token_at - self.started) if self.last_token_at is not None else 0.0
        )
        GATEWAY_OVERHEAD.observe(max(0.0, total - provider_time))


@asynccontextmanager
async def track_inflight(provider: str) -> AsyncIterator[None]:
    INFLIGHT.labels(provider=provider).inc()
    try:
        yield
    finally:
        INFLIGHT.labels(provider=provider).dec()

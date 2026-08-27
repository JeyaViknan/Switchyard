"""Tests for the request timing model.

The property that matters is that the decomposition is exhaustive: total is
fully accounted for by queue wait, provider time, and gateway overhead. If it
were not, work could disappear from the observability model without anything
failing -- which is exactly what the previous definition of gateway_overhead did
by reporting only the gap after the last token.
"""

from __future__ import annotations

import time

import pytest

from switchyard.obs.metrics import REGISTRY, RequestTimeline


def histogram(name: str) -> tuple[float, float]:
    """Return (sum, count) for a histogram, across all label sets."""
    total = count = 0.0
    for metric in REGISTRY.collect():
        if metric.name != name:
            continue
        for sample in metric.samples:
            if sample.name.endswith("_sum"):
                total += sample.value
            elif sample.name.endswith("_count"):
                count += sample.value
    return total, count


def delta(name: str, before: tuple[float, float]) -> float:
    """The single observation added to `name` since `before`."""
    total, count = histogram(name)
    assert count - before[1] == 1, f"expected exactly one new observation on {name}"
    return total - before[0]


def test_gateway_overhead_excludes_provider_time():
    """The bug this replaces: overhead used to be the gap after the last token.

    A request that spends 200ms waiting on a provider and does trivial work of
    its own must report near-zero overhead, not 200ms and not the trailing gap.
    """
    before = histogram("switchyard_gateway_overhead_seconds")

    timeline = RequestTimeline()
    timeline.mark_dispatched()
    time.sleep(0.20)                      # provider time
    for _ in range(3):
        timeline.on_token("p")
    timeline.mark_provider_done()
    timeline.finish("m", "p", "ok")

    overhead = delta("switchyard_gateway_overhead_seconds", before)
    assert overhead < 0.05, f"provider time leaked into overhead: {overhead * 1000:.1f}ms"


def test_gateway_work_before_dispatch_is_counted_as_overhead():
    before = histogram("switchyard_gateway_overhead_seconds")

    timeline = RequestTimeline()
    time.sleep(0.15)                      # parse / validate / route
    timeline.mark_dispatched()
    timeline.on_token("p")
    timeline.mark_provider_done()
    timeline.finish("m", "p", "ok")

    assert delta("switchyard_gateway_overhead_seconds", before) >= 0.15


def test_queue_wait_is_recorded_and_removed_from_overhead():
    """Queue wait must be its own span, not absorbed into gateway overhead.

    Week 2 introduces real queueing; if it landed in overhead, the scheduler
    would appear to make the gateway slower rather than making requests wait.
    """
    qbefore = histogram("switchyard_queue_wait_seconds")
    obefore = histogram("switchyard_gateway_overhead_seconds")

    timeline = RequestTimeline()
    timeline.record_queue_wait(0.30)      # as the admission controller will
    timeline.mark_dispatched()
    timeline.on_token("p")
    timeline.mark_provider_done()
    timeline.finish("m", "p", "ok")

    assert delta("switchyard_queue_wait_seconds", qbefore) == pytest.approx(0.30)
    # The 300ms was never really slept, so subtracting it drives the residual to
    # the clamp rather than negative.
    assert delta("switchyard_gateway_overhead_seconds", obefore) == 0.0


def test_decomposition_sums_to_total():
    """total == queue_wait + provider_time + gateway_overhead, by construction."""
    marks = {
        n: histogram(n)
        for n in (
            "switchyard_request_duration_seconds",
            "switchyard_queue_wait_seconds",
            "switchyard_provider_time_seconds",
            "switchyard_gateway_overhead_seconds",
        )
    }

    timeline = RequestTimeline()
    time.sleep(0.05)                      # gateway work before dispatch
    timeline.mark_dispatched()
    time.sleep(0.10)                      # provider
    timeline.on_token("p")
    timeline.mark_provider_done()
    time.sleep(0.02)                      # gateway work after the provider
    timeline.finish("m", "p", "ok")

    got = {n: delta(n, m) for n, m in marks.items()}
    total = got["switchyard_request_duration_seconds"]
    queue = got["switchyard_queue_wait_seconds"]
    provider = got["switchyard_provider_time_seconds"]
    overhead = got["switchyard_gateway_overhead_seconds"]

    assert queue + provider + overhead == pytest.approx(total, abs=1e-6)
    assert provider == pytest.approx(0.10, abs=0.03)
    assert overhead == pytest.approx(0.07, abs=0.03)


def test_ttft_is_measured_from_arrival_not_dispatch():
    """A queued request is slow to first token; the metric must say so."""
    before = histogram("switchyard_ttft_seconds")

    timeline = RequestTimeline()
    time.sleep(0.10)                      # time before the provider is called
    timeline.mark_dispatched()
    timeline.on_token("p")
    timeline.mark_provider_done()
    timeline.finish("m", "p", "ok")

    assert delta("switchyard_ttft_seconds", before) >= 0.10


def test_request_rejected_before_dispatch_is_all_gateway_time():
    before = histogram("switchyard_provider_time_seconds")

    timeline = RequestTimeline()
    timeline.finish("m", "none", "error")   # never dispatched

    assert delta("switchyard_provider_time_seconds", before) == 0.0

"""Tests for the open-loop load generator.

The load generator is the instrument every later claim rests on, so these tests
target the properties that make its numbers trustworthy rather than merely
checking that it runs.
"""

from __future__ import annotations

import numpy as np
import pytest

from switchyard.bench.loadgen import LoadSpec, RequestRecord, arrival_schedule, run_load
from switchyard.bench.stats import percentiles, summarize
from switchyard.synthetic.profiles import FaultSpec

# -- arrival schedule ------------------------------------------------------


def test_schedule_matches_requested_rate():
    times = arrival_schedule(rate=50.0, duration_s=10.0, seed=1)
    assert 400 < len(times) < 600            # ~500 arrivals, Poisson variance
    assert times.max() < 10.0


def test_schedule_is_deterministic_and_monotonic():
    a = arrival_schedule(20.0, 5.0, seed=7)
    b = arrival_schedule(20.0, 5.0, seed=7)
    assert np.array_equal(a, b)
    assert np.all(np.diff(a) > 0)


def test_schedule_gaps_are_exponential_not_uniform():
    """Poisson arrivals, not a fixed-interval drumbeat.

    A fixed-interval generator produces artificially smooth queueing behavior;
    real arrivals are bursty, and burstiness is what stresses admission control.
    """
    gaps = np.diff(arrival_schedule(100.0, 40.0, seed=3))
    # For an exponential distribution the std equals the mean.
    assert 0.8 < gaps.std() / gaps.mean() < 1.2


# -- the open-loop property ------------------------------------------------


async def test_generator_is_open_loop_not_closed_loop(fleet_server):
    """Arrivals must not be throttled by how slowly the system responds.

    Against a target that takes ~200 ms per request, a closed-loop generator
    with a small worker pool would issue only a handful of requests per second.
    An open-loop generator issues the full scheduled count regardless.
    """
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/held/chat/completions",
        rate=40.0, duration_s=1.0, model="held", seed=5,
    )
    expected = len(arrival_schedule(spec.rate, spec.duration_s, spec.seed))
    records = await run_load(spec)

    assert len(records) == expected
    assert expected > 25, "test needs enough arrivals to distinguish the two models"
    assert all(r.ok for r in records)


async def test_latency_is_measured_from_intended_start(fleet_server):
    """Latency must include time the request spent waiting to be issued.

    This is the coordinated-omission guard: measuring from `actual_start` would
    silently discard queueing delay.
    """
    record = RequestRecord(
        request_id="x", tenant="t1", intended_start=100.0, actual_start=100.5
    )
    record.first_token_at = 101.0
    record.completed_at = 102.0

    assert record.scheduling_lag == pytest.approx(0.5)
    assert record.ttft == pytest.approx(1.0)      # from intended, not actual
    assert record.latency == pytest.approx(2.0)


async def test_scheduling_lag_is_reported_so_invalid_runs_are_detectable(fleet_server):
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=20.0, duration_s=1.0, model="quick", seed=2,
    )
    summary = summarize(await run_load(spec))
    assert "scheduling_lag_p99_ms" in summary
    assert summary["generator_kept_up"] is True


# -- SSE parsing -----------------------------------------------------------


async def test_counts_tokens_and_captures_finish_reason(fleet_server):
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=5.0, duration_s=0.5, model="quick", seed=11,
    )
    records = await run_load(spec)
    assert records
    for r in records:
        assert r.ok
        assert r.output_tokens > 0
        assert r.finish_reason == "stop"
        assert r.ttft is not None and r.ttft > 0


async def test_truncated_stream_is_recorded_as_an_error_not_a_success(fleet_server):
    """A stream that stops without [DONE] must never look like a clean completion."""
    fleet_server.state.profiles["quick"] = fleet_server.state.profiles["quick"].with_faults(
        FaultSpec(abort_rate=1.0, abort_after_chunks=2)
    )
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=5.0, duration_s=0.4, model="quick", seed=13,
    )
    records = await run_load(spec)
    assert records
    assert all(r.error == "truncated" for r in records)
    assert all(not r.ok for r in records)


async def test_http_error_is_recorded_with_status(fleet_server):
    fleet_server.state.profiles["quick"] = fleet_server.state.profiles["quick"].with_faults(
        FaultSpec(error_rate=1.0)
    )
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=5.0, duration_s=0.4, model="quick", seed=17,
    )
    records = await run_load(spec)
    assert records
    assert all(r.status == 500 and r.error == "http_500" for r in records)
    assert all(r.ttft is None for r in records)


# -- summary ---------------------------------------------------------------


def test_percentiles_on_empty_input_are_nan_not_zero():
    """Zero would silently read as 'very fast'; NaN forces the absence to show."""
    out = percentiles([])
    assert all(np.isnan(v) for v in out.values())


def test_summary_separates_throughput_from_goodput():
    records = []
    for i in range(10):
        r = RequestRecord(request_id=f"r{i}", tenant="t1", intended_start=0.0, actual_start=0.0)
        r.status = 200
        r.first_token_at = 0.1
        r.completed_at = 1.0 if i < 6 else 30.0    # 4 of 10 blow the SLO
        records.append(r)
    summary = summarize(records, slo_s=10.0)
    assert summary["completed_ok"] == 10
    assert summary["goodput_rps"] < summary["throughput_rps"]

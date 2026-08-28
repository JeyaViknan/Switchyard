"""Tests for the open-loop load generator.

The load generator is the instrument every later claim rests on, so these tests
target the properties that make its numbers trustworthy rather than merely
checking that it runs.
"""

from __future__ import annotations

import resource

import numpy as np
import pytest

from switchyard.bench.loadgen import (
    LoadSpec,
    RequestRecord,
    RunOutcome,
    arrival_schedule,
    check_fd_headroom,
    request_id_for,
    run_load,
    run_load_detailed,
)
from switchyard.bench.stats import percentiles, read_parquet, summarize, write_parquet
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


async def test_generator_health_reflects_lag_relative_to_the_workload(fleet_server):
    """The same absolute lag is fine for a slow workload and disqualifying for a fast one.

    Both arms are real runs at the same rate against the same process. Only the
    provider's speed differs, and that alone flips the verdict -- which is the
    point of judging lag as a ratio rather than against a fixed millisecond bar.
    """
    def spec_for(model: str) -> LoadSpec:
        return LoadSpec(
            url=f"{fleet_server.base_url}/v1/{model}/chat/completions",
            rate=20.0, duration_s=1.0, model=model, seed=2,
        )

    slow = await run_load_detailed(spec_for("held"))       # ~200ms requests
    slow_summary = summarize(slow.records, outcome=slow)
    assert slow_summary["generator_healthy"] is True
    assert slow_summary["generator_problems"] == "-"

    fast = await run_load_detailed(spec_for("quick"))      # ~10ms requests
    fast_summary = summarize(fast.records, outcome=fast)
    assert fast_summary["scheduling_lag_ratio"] > slow_summary["scheduling_lag_ratio"]


def test_generator_health_is_a_ratio_of_latency_not_a_fixed_threshold():
    """20ms of lag is negligible against a 2s request and fatal against a 50ms one."""
    def run(lag_s: float, latency_s: float):
        records = []
        for i in range(50):
            r = RequestRecord(request_id=f"r{i}", tenant="t", intended_start=0.0,
                              actual_start=lag_s)
            r.status = 200
            r.first_token_at = lag_s
            r.completed_at = latency_s
            records.append(r)
        return summarize(records)

    assert run(lag_s=0.02, latency_s=2.0)["generator_healthy"] is True
    slow = run(lag_s=0.02, latency_s=0.05)
    assert slow["generator_healthy"] is False
    assert "understates" in slow["generator_problems"][0]


def test_connection_saturation_marks_the_run_unhealthy():
    """At the connection cap the generator is no longer open-loop, and says so."""
    records = []
    for i in range(20):
        r = RequestRecord(request_id=f"r{i}", tenant="t", intended_start=0.0, actual_start=0.0)
        r.status = 200
        r.first_token_at = 0.01
        r.completed_at = 1.0
        records.append(r)

    saturated = RunOutcome(records=records, peak_concurrency=8, connection_limited=True)
    summary = summarize(records, outcome=saturated)
    assert summary["generator_healthy"] is False
    assert any("open-loop" in p for p in summary["generator_problems"])
    assert summary["peak_concurrency"] == 8


def test_fd_headroom_is_checked_before_the_run_starts():
    """fd exhaustion must not masquerade as the system under test refusing work."""
    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY:
        pytest.skip("no descriptor limit on this platform")
    with pytest.raises(RuntimeError, match="file descriptors"):
        check_fd_headroom(soft + 1000)
    check_fd_headroom(1)          # comfortably under the limit; must not raise


# -- measurement window ----------------------------------------------------


async def test_warmup_requests_are_issued_but_excluded_from_statistics(fleet_server):
    """Warmup keeps the system warm without letting cold-start costs into the numbers."""
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=20.0, duration_s=1.0, model="quick", seed=3, warmup_s=0.4,
    )
    records = await run_load(spec)
    warmup = [r for r in records if not r.in_window]
    measured = [r for r in records if r.in_window]

    assert warmup and measured, "test needs requests on both sides of the boundary"
    assert all(r.completed_at is not None for r in warmup), "warmup requests still run"

    summary = summarize(records)
    assert summary["requests_total"] == len(records)
    assert summary["requests_in_window"] == len(measured)
    assert summary["requests_in_window"] < summary["requests_total"]


async def test_zero_warmup_includes_every_request(fleet_server):
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=10.0, duration_s=0.6, model="quick", seed=3,
    )
    summary = summarize(await run_load(spec))
    assert summary["requests_in_window"] == summary["requests_total"]


# -- inter-token timing ----------------------------------------------------


async def test_inter_token_samples_survive_persistence(tmp_path, fleet_server):
    """A mean cannot express an inter-token tail, so the samples must be kept."""
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=8.0, duration_s=0.5, model="quick", seed=21,
    )
    records = await run_load(spec)
    path = str(tmp_path / "itl.parquet")
    write_parquet(records, path, label="itl", spec=spec)

    table = read_parquet(path)
    assert "inter_token_s" in table.column_names
    rows = table["inter_token_s"].to_pylist()
    pooled = [gap for row in rows if row for gap in row]
    # More gaps than requests proves per-token samples survived, rather than one
    # aggregate per request.
    assert len(pooled) > len(rows) > 1
    assert float(np.percentile(pooled, 99)) >= float(np.percentile(pooled, 50))


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


# -- reproducibility -------------------------------------------------------


def test_request_ids_are_deterministic_from_seed_and_index():
    assert request_id_for(7, 0) == request_id_for(7, 0)
    assert request_id_for(7, 0) != request_id_for(7, 1)
    assert request_id_for(7, 0) != request_id_for(8, 0)


async def test_same_seed_reproduces_the_same_workload(fleet_server):
    """Two runs of one spec must be the same workload, request for request.

    The synthetic fleet keys every draw on (run_seed, request_id). If ids were
    random, two runs would differ in output length, TTFT and fault decisions --
    so comparing two scheduling policies would compare two different workloads,
    and every A/B result in the project would be meaningless.
    """
    spec = LoadSpec(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=15.0, duration_s=1.0, model="quick", seed=4242, max_tokens=512,
    )
    first = await run_load(spec)
    second = await run_load(spec)

    assert len(first) == len(second) > 5
    assert [r.request_id for r in first] == [r.request_id for r in second]
    assert [r.output_tokens for r in first] == [r.output_tokens for r in second]


async def test_different_seed_produces_a_different_workload(fleet_server):
    base = dict(
        url=f"{fleet_server.base_url}/v1/quick/chat/completions",
        rate=15.0, duration_s=1.0, model="quick", max_tokens=512,
    )
    a = await run_load(LoadSpec(seed=1, **base))
    b = await run_load(LoadSpec(seed=2, **base))
    assert [r.output_tokens for r in a] != [r.output_tokens for r in b]


# -- failures the gateway signals inside a well-formed stream ---------------


async def test_a_gateway_error_frame_is_counted_as_a_failure(fleet_server):
    """A provider failure arrives as a normal-looking stream ending in [DONE].

    The terminal frame carries the error. Judging success by stream shape alone
    counted every provider outage as a completed request, which made an outage
    benchmark report 100% success while the provider was returning 5xx.
    """
    from tests.conftest import serve_gateway

    from switchyard.core.auth import mint_key
    from switchyard.core.config import BreakerConfig, GatewayConfig, Tenant

    raw, digest = mint_key("t1")
    config = GatewayConfig(
        max_concurrency=4, providers=("quick", "held"),
        routes={"quick": ("quick",)},
        breaker=BreakerConfig(min_samples=1000, window=1000),   # never trips here
        tenants=(Tenant(id="t1", key_sha256=digest),),
    )
    config.validate()

    fleet_server.state.profiles["quick"] = fleet_server.state.profiles["quick"].with_faults(
        FaultSpec(error_rate=1.0)
    )
    async with serve_gateway(config, fleet_server, {"t1": raw}) as gw:
        records = await run_load(LoadSpec(
            url=f"{gw.base_url}/v1/chat/completions", rate=8.0, duration_s=0.6,
            model="quick", tenants=("t1",), seed=5, api_key=raw,
        ))

    assert records
    assert all(r.status == 200 for r in records), "the SSE response itself is well-formed"
    assert all(not r.ok for r in records), "but every request failed and must count as one"
    assert all(r.finish_reason == "provider_error" for r in records)

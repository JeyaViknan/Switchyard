"""Tests for the synthetic provider fleet.

The fleet is an instrument, so these tests are about the properties that make it
usable as one: determinism, well-formed streams, and faults that fail in the
specific way each experiment needs to distinguish.
"""

from __future__ import annotations

import time

import pytest
from httpx import ASGITransport, AsyncClient

from switchyard.synthetic.app import REQUEST_ID_HEADER, FleetState, create_app
from switchyard.synthetic.profiles import FaultSpec, ProviderProfile, plan_request
from switchyard.types import ErrorClass

# Fast profile so the suite does not spend real time asleep.
FAST = ProviderProfile(
    name="t",
    ttft_median_ms=1.0,
    ttft_sigma=0.01,
    output_tokens_median=20.0,
    output_tokens_sigma=0.01,
    tokens_per_second=10_000.0,
    token_jitter_sigma=0.0,
)


def fleet(profile: ProviderProfile = FAST, seed: int = 1) -> FleetState:
    return FleetState({"t": profile}, run_seed=seed)


def client(state: FleetState) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=create_app(state)), base_url="http://fleet")


async def collect(state: FleetState, request_id: str = "r1", **body_over):
    """Run one streaming request, returning (status, sse_lines)."""
    body = {"model": "t", "messages": [{"role": "user", "content": "hello"}],
            "stream": True, "max_tokens": 4096} | body_over
    async with client(state) as c, c.stream(
        "POST", "/v1/t/chat/completions", json=body,
        headers={REQUEST_ID_HEADER: request_id},
    ) as r:
        if r.status_code != 200:
            await r.aread()
            return r.status_code, []
        lines = [ln for ln in [x async for x in r.aiter_lines()] if ln.startswith("data: ")]
        return r.status_code, lines


# -- determinism -----------------------------------------------------------

def test_plan_is_deterministic_for_same_seed_and_request_id():
    a = plan_request(FAST, 7, "req-x", 4096)
    b = plan_request(FAST, 7, "req-x", 4096)
    assert a == b


def test_plan_differs_across_request_ids():
    a = plan_request(FAST, 7, "req-x", 4096)
    b = plan_request(FAST, 7, "req-y", 4096)
    assert (a.n_tokens, a.ttft_s) != (b.n_tokens, b.ttft_s)


def test_plan_differs_across_run_seeds():
    a = plan_request(FAST, 7, "req-x", 4096)
    b = plan_request(FAST, 8, "req-x", 4096)
    assert (a.n_tokens, a.ttft_s) != (b.n_tokens, b.ttft_s)


def test_output_length_is_clipped_to_max_tokens():
    plan = plan_request(FAST, 7, "req-x", max_tokens=5)
    assert 1 <= plan.n_tokens <= 5


def test_fault_configuration_does_not_shift_the_draw_sequence():
    """Enabling a fault must not change the request's other properties.

    If it did, an experiment comparing a fault-free run against a faulted one
    would be comparing two different workloads.
    """
    clean = plan_request(FAST, 7, "req-x", 4096)
    faulted = plan_request(FAST.with_faults(FaultSpec(abort_rate=1.0)), 7, "req-x", 4096)
    assert (clean.n_tokens, clean.ttft_s) == (faulted.n_tokens, faulted.ttft_s)


# -- stream shape ----------------------------------------------------------

async def test_stream_is_well_formed_and_terminates():
    status, lines = await collect(fleet())
    assert status == 200
    assert lines[-1] == "data: [DONE]"
    assert '"finish_reason":"stop"' in lines[-2].replace(" ", "")


async def test_emitted_chunk_count_matches_plan():
    plan = plan_request(FAST, 1, "r1", 4096)
    _, lines = await collect(fleet())
    content_frames = [ln for ln in lines if '"content"' in ln]
    assert len(content_frames) == plan.n_tokens


async def test_usage_reports_completion_tokens_actually_sent():
    plan = plan_request(FAST, 1, "r1", 4096)
    _, lines = await collect(fleet())
    assert f'"completion_tokens":{plan.n_tokens}' in lines[-2].replace(" ", "")


async def test_hitting_max_tokens_reports_finish_reason_length():
    _, lines = await collect(fleet(), max_tokens=3)
    assert '"finish_reason":"length"' in lines[-2].replace(" ", "")


async def test_non_streaming_is_rejected_loudly():
    async with client(fleet()) as c:
        r = await c.post("/v1/t/chat/completions",
                         json={"model": "t", "messages": [], "stream": False})
    assert r.status_code == 400
    assert "streaming-only" in r.text


async def test_unknown_provider_is_404():
    async with client(fleet()) as c:
        r = await c.post("/v1/nope/chat/completions",
                         json={"model": "x", "messages": [], "stream": True})
    assert r.status_code == 404


# -- faults ----------------------------------------------------------------

@pytest.mark.parametrize(
    ("error_class", "expected_status"),
    [(ErrorClass.SERVER_ERROR, 500), (ErrorClass.RATE_LIMITED, 429),
     (ErrorClass.BAD_REQUEST, 400)],
)
async def test_pre_first_token_failure_surfaces_as_http_status(error_class, expected_status):
    """The recoverable failure class must arrive before the response body starts."""
    profile = FAST.with_faults(FaultSpec(error_rate=1.0, error_class=error_class))
    status, lines = await collect(fleet(profile))
    assert status == expected_status
    assert lines == []


async def test_mid_stream_abort_ends_without_a_terminal_frame():
    """The ambiguous truncation case: bytes stop, no finish_reason, no [DONE]."""
    profile = FAST.with_faults(FaultSpec(abort_rate=1.0, abort_after_chunks=5))
    _, lines = await collect(fleet(profile))
    assert len(lines) == 5
    assert all('"content"' in ln for ln in lines)
    assert "data: [DONE]" not in lines


async def test_mid_stream_stall_delays_without_closing():
    profile = FAST.with_faults(
        FaultSpec(stall_rate=1.0, stall_after_chunks=2, stall_seconds=0.25)
    )
    start = time.perf_counter()
    status, lines = await collect(fleet(profile))
    elapsed = time.perf_counter() - start
    assert status == 200
    assert lines[-1] == "data: [DONE]"
    assert elapsed >= 0.25


async def test_ttft_delay_is_respected():
    slow_start = ProviderProfile(
        name="t", ttft_median_ms=200.0, ttft_sigma=0.001,
        output_tokens_median=2.0, output_tokens_sigma=0.001,
        tokens_per_second=10_000.0, token_jitter_sigma=0.0,
    )
    start = time.perf_counter()
    await collect(fleet(slow_start))
    assert time.perf_counter() - start >= 0.15


# -- control plane ---------------------------------------------------------

async def test_control_patch_updates_faults_and_reset_restores_defaults():
    state = fleet()
    async with client(state) as c:
        r = await c.put("/control/profiles/t", json={"faults": {"error_rate": 1.0}})
        assert r.status_code == 200
        assert r.json()["faults"]["error_rate"] == 1.0
        assert state.profiles["t"].faults.error_rate == 1.0

        assert (await c.post("/control/reset")).status_code == 200
        assert state.profiles["t"].faults.error_rate == 0.0


async def test_control_rejects_out_of_range_fault_probability():
    async with client(fleet()) as c:
        with pytest.raises(ValueError):
            await c.put("/control/profiles/t", json={"faults": {"error_rate": 1.5}})


async def test_changing_run_seed_changes_behavior():
    state = fleet()
    async with client(state) as c:
        assert (await c.put("/control/seed", json={"run_seed": 99})).json()["run_seed"] == 99
    assert state.run_seed == 99
    assert plan_request(FAST, 1, "r1", 4096) != plan_request(FAST, 99, "r1", 4096)

"""Gateway tests.

Week 1's gateway is a passthrough, so these cover the contract it must honor
regardless of what is added later: reject what is not supported, always
terminate a stream explicitly, and never report a truncated response as clean.
"""

from __future__ import annotations

import httpx
import pytest

from switchyard.synthetic.profiles import FaultSpec

BODY = {"model": "quick", "messages": [{"role": "user", "content": "hello"}], "max_tokens": 64}


async def sse_lines(url: str, body: dict) -> tuple[int, list[str]]:
    async with (
        httpx.AsyncClient(timeout=30.0) as c,
        c.stream("POST", url, json=body | {"stream": True}) as r,
    ):
            if r.status_code != 200:
                await r.aread()
                return r.status_code, []
            return r.status_code, [
                ln for ln in [x async for x in r.aiter_lines()] if ln.startswith("data: ")
            ]


# -- request validation ----------------------------------------------------


@pytest.mark.parametrize(
    ("patch", "fragment"),
    [
        ({"tools": [{"type": "function"}]}, "tools"),
        ({"response_format": {"type": "json"}}, "response_format"),
        ({"n": 3}, "n=1"),
        ({"messages": []}, "non-empty"),
        ({"model": ""}, "'model' is required"),
        ({"temperature": 5.0}, "temperature"),
        ({"max_tokens": 0}, "max_tokens"),
    ],
)
async def test_unsupported_or_invalid_fields_are_rejected_by_name(gateway_server, patch, fragment):
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{gateway_server.base_url}/v1/chat/completions", json=BODY | patch)
    assert r.status_code == 400
    assert fragment in r.text


async def test_unknown_model_is_404(gateway_server):
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{gateway_server.base_url}/v1/chat/completions", json=BODY | {"model": "nope"}
        )
    assert r.status_code == 404


async def test_oversized_prompt_rejected_before_reaching_a_provider(gateway_server):
    huge = {"role": "user", "content": "x" * 200_000}
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{gateway_server.base_url}/v1/chat/completions", json=BODY | {"messages": [huge]}
        )
    assert r.status_code == 400
    assert "prompt too large" in r.text


# -- streaming -------------------------------------------------------------


async def test_stream_terminates_with_finish_reason_and_done(gateway_server):
    status, lines = await sse_lines(f"{gateway_server.base_url}/v1/chat/completions", BODY)
    assert status == 200
    assert lines[-1] == "data: [DONE]"
    assert '"finish_reason":"stop"' in lines[-2].replace(" ", "")
    assert any('"content"' in ln for ln in lines[:-2])


async def test_non_streaming_is_assembled_from_the_same_stream(gateway_server):
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"{gateway_server.base_url}/v1/chat/completions", json=BODY)
    assert r.status_code == 200
    payload = r.json()
    assert payload["object"] == "chat.completion"
    assert payload["choices"][0]["message"]["content"]
    assert payload["usage"]["completion_tokens"] > 0
    assert payload["choices"][0]["finish_reason"] == "stop"


# -- failure semantics -----------------------------------------------------


async def test_provider_error_becomes_a_typed_terminal_frame_not_a_dropped_stream(gateway_server):
    """Invariant I8: the client always learns how the stream ended."""
    fleet = gateway_server.fleet.state
    fleet.profiles["quick"] = fleet.profiles["quick"].with_faults(FaultSpec(error_rate=1.0))

    status, lines = await sse_lines(f"{gateway_server.base_url}/v1/chat/completions", BODY)
    assert status == 200                       # the gateway responded; the provider failed
    assert lines[-1] == "data: [DONE]"
    body = lines[-2].replace(" ", "")
    assert '"finish_reason":"provider_error"' in body
    assert '"tokens_emitted":0' in body


async def test_truncated_provider_stream_is_reported_as_an_error(gateway_server):
    """A provider that stops mid-stream must not look like a clean completion."""
    fleet = gateway_server.fleet.state
    fleet.profiles["quick"] = fleet.profiles["quick"].with_faults(
        FaultSpec(abort_rate=1.0, abort_after_chunks=3)
    )
    status, lines = await sse_lines(f"{gateway_server.base_url}/v1/chat/completions", BODY)

    assert status == 200
    assert lines[-1] == "data: [DONE]"
    body = lines[-2].replace(" ", "")
    assert '"finish_reason":"provider_error"' in body
    assert '"tokens_emitted":3' in body        # exactly what reached the client
    content_frames = [ln for ln in lines if '"content"' in ln]
    assert len(content_frames) == 3


async def test_non_streaming_surfaces_provider_failure_in_the_payload(gateway_server):
    fleet = gateway_server.fleet.state
    fleet.profiles["quick"] = fleet.profiles["quick"].with_faults(
        FaultSpec(abort_rate=1.0, abort_after_chunks=2)
    )
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"{gateway_server.base_url}/v1/chat/completions", json=BODY)
    payload = r.json()
    assert payload["choices"][0]["finish_reason"] == "provider_error"
    assert payload["error"]["tokens_emitted"] == 2


# -- observability ---------------------------------------------------------


async def test_metrics_endpoint_exposes_switchyard_series(gateway_server):
    await sse_lines(f"{gateway_server.base_url}/v1/chat/completions", BODY)
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{gateway_server.base_url}/metrics")
    assert r.status_code == 200
    for series in (
        "switchyard_requests_total",
        "switchyard_ttft_seconds",
        "switchyard_gateway_overhead_seconds",
        "switchyard_event_loop_lag_seconds",
    ):
        assert series in r.text

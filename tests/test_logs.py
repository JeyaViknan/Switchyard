"""Structured logging.

Two things matter here. The events have to carry enough to reconstruct what
happened, and they must never carry what was said -- prompts, responses, or
credentials. The second is the one worth a test, because it is the failure
nobody notices until the logs are already somewhere they should not be.
"""

from __future__ import annotations

import io
import json
import logging

import pytest

from switchyard.obs.logs import (
    FORBIDDEN_FIELDS,
    JsonFormatter,
    RedactionError,
    TextFormatter,
    configure,
    event,
    formatter_for,
    get_logger,
)


def capture(fmt: str = "json", level: str = "DEBUG") -> tuple[logging.Logger, io.StringIO]:
    buffer = io.StringIO()
    configure(fmt=fmt, level=level, stream=buffer)
    return get_logger("test"), buffer


def records(buffer: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line]


# -- structure -------------------------------------------------------------


def test_an_event_carries_its_name_level_and_fields():
    log, buffer = capture()
    event(log, "request.rejected", tenant="acme", reason="queue_full", queued=128)

    entry = records(buffer)[0]
    assert entry["event"] == "request.rejected"
    assert entry["level"] == "info"
    assert entry["tenant"] == "acme" and entry["reason"] == "queue_full"
    assert entry["queued"] == 128
    assert entry["ts"].endswith("Z")


def test_text_format_is_readable_on_one_line():
    log, buffer = capture(fmt="text")
    event(log, "provider.failover", **{"from": "fast"}, to="slow", reason="server_error")

    line = buffer.getvalue().strip()
    assert "\n" not in line
    assert "provider.failover" in line
    assert "from=fast" in line and "to=slow" in line


def test_format_is_selected_by_name():
    assert isinstance(formatter_for("json"), JsonFormatter)
    assert isinstance(formatter_for("text"), TextFormatter)
    assert isinstance(formatter_for("anything else"), TextFormatter)


def test_configure_is_idempotent_and_does_not_duplicate_lines():
    buffer = io.StringIO()
    for _ in range(3):
        configure(fmt="json", level="DEBUG", stream=buffer)
    event(get_logger("test"), "gateway.started")
    assert len(records(buffer)) == 1


def test_switchyard_logs_do_not_propagate_to_the_root_handler():
    """Otherwise every line appears twice once uvicorn configures logging."""
    configure(fmt="json", stream=io.StringIO())
    assert logging.getLogger("switchyard").propagate is False


# -- levels carry meaning --------------------------------------------------


def test_per_request_lines_are_debug_and_off_by_default():
    """One line per request is unreadable under load, so INFO stays quiet."""
    log, buffer = capture(level="INFO")
    event(log, "request.completed", logging.DEBUG, request_id="r1", tokens=164)
    event(log, "request.rejected", tenant="acme", reason="queue_full")

    names = [r["event"] for r in records(buffer)]
    assert names == ["request.rejected"], "only notable events at the default level"


def test_debug_level_reveals_per_request_lines():
    log, buffer = capture(level="DEBUG")
    event(log, "request.completed", logging.DEBUG, request_id="r1", tokens=164)
    assert records(buffer)[0]["request_id"] == "r1"


# -- nothing sensitive -----------------------------------------------------


@pytest.mark.parametrize("field", sorted(FORBIDDEN_FIELDS))
def test_content_and_credential_fields_are_refused(field):
    """A careless call site should fail loudly rather than leak quietly."""
    log, _ = capture()
    with pytest.raises(RedactionError, match=field):
        event(log, "request.completed", **{field: "secret"})


def test_the_refusal_names_the_offending_field():
    log, _ = capture()
    with pytest.raises(RedactionError) as exc:
        event(log, "x", prompt="hello", tenant="acme")
    assert "prompt" in str(exc.value)
    assert "never logged" in str(exc.value)


def test_a_normal_request_event_records_shape_not_content():
    """Counts and durations are enough to reconstruct what happened."""
    log, buffer = capture(level="DEBUG")
    event(log, "request.completed", logging.DEBUG,
          request_id="sy-abc", tenant="acme", model="fast", provider="slow",
          tokens=164, queue_wait_ms=41.0, attempts=2, failed_over=True)

    entry = records(buffer)[0]
    assert entry["tokens"] == 164 and entry["failed_over"] is True
    assert not any(k in entry for k in FORBIDDEN_FIELDS)


# -- events the product actually emits -------------------------------------


async def test_a_rejected_request_is_logged_with_its_reason(fleet_server):
    """The log should say which limit refused the request, not just that one did."""
    import httpx
    from tests.conftest import build_tenant_config, serve_gateway

    buffer = io.StringIO()
    configure(fmt="json", level="INFO", stream=buffer)

    config, keys = build_tenant_config(
        max_concurrency=1, policy="drr", max_queue_depth=1, deadline_s=30.0
    )
    body = {"model": "held", "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 64, "stream": True}
    async with serve_gateway(config, fleet_server, keys) as gw:
        import asyncio

        held = [asyncio.create_task(
            httpx.AsyncClient(timeout=30.0).post(
                f"{gw.base_url}/v1/chat/completions", json=body, headers=gw.auth("alpha")
            )
        ) for _ in range(3)]
        await asyncio.sleep(0.3)
        async with httpx.AsyncClient(timeout=30.0) as c:
            await c.post(f"{gw.base_url}/v1/chat/completions",
                         json=body | {"stream": False}, headers=gw.auth("alpha"))
        for task in held:
            task.cancel()
        await asyncio.gather(*held, return_exceptions=True)

    rejected = [r for r in records(buffer) if r["event"] == "request.rejected"]
    assert rejected, "a refused request should be logged"
    assert rejected[0]["reason"] == "queue_full"
    assert rejected[0]["tenant"] == "alpha"


def test_breaker_transitions_are_logged():
    from switchyard.core.health import BreakerPolicy, ProviderHealth
    from switchyard.types import ErrorClass

    _, buffer = capture(level="INFO")
    health = ProviderHealth(name="fast", policy=BreakerPolicy(min_samples=2, cooldown_s=1.0))
    for _ in range(2):
        health.record_failure(ErrorClass.SERVER_ERROR)

    opened = [r for r in records(buffer) if r["event"] == "breaker.opened"]
    assert opened and opened[0]["provider"] == "fast"
    assert opened[0]["retry_in_s"] > 0


def test_draining_twice_logs_one_event():
    """An explicit drain is normally followed by the lifespan's own at shutdown."""
    import asyncio

    from switchyard.core.config import GatewayConfig, Tenant
    from switchyard.core.scheduler import Scheduler

    _, buffer = capture(level="INFO")
    config = GatewayConfig(tenants=(Tenant(id="t1", key_sha256="a" * 64),))
    scheduler = Scheduler(config)

    async def drain_twice():
        await scheduler.drain(timeout_s=0.1)
        await scheduler.drain(timeout_s=0.1)

    asyncio.run(drain_twice())
    started = [r for r in records(buffer) if r["event"] == "drain.started"]
    assert len(started) == 1

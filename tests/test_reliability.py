"""Reliability behaviour through the full gateway.

The unit tests cover the breaker and the router in isolation. These check the
interactions that only exist once everything is wired: that a failover reuses
the same capacity lease and budget reservation rather than taking a second of
each, and that a failure after capacity has been acquired still releases it.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from tests.conftest import build_tenant_config, serve_gateway

from switchyard.core.config import BreakerConfig, TimeoutPolicy
from switchyard.synthetic.profiles import FaultSpec, ProviderProfile
from switchyard.types import ErrorClass

BODY = {"model": "quick", "messages": [{"role": "user", "content": "hi"}], "max_tokens": 64}


def routed_config(**over):
    """Two tenants, with 'quick' able to fall back to 'held'."""
    from dataclasses import replace

    config, keys = build_tenant_config(max_concurrency=4, **over.pop("tenant", {}))
    config = replace(
        config,
        providers=("quick", "held"),
        routes={"quick": ("quick", "held"), "held": ("held",)},
        timeouts=over.pop("timeouts", TimeoutPolicy(ttft_s=1.0, inter_token_s=1.0)),
        breaker=over.pop("breaker", BreakerConfig(min_samples=4, cooldown_s=30.0)),
        **over,
    )
    config.validate()
    return config, keys


async def post(gw, tenant, body=None, **kw):
    async with httpx.AsyncClient(timeout=30.0) as c:
        return await c.post(f"{gw.base_url}/v1/chat/completions",
                            json=body or BODY, headers=gw.auth(tenant), **kw)


async def scheduler_stats(gw) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as c:
        return (await c.get(f"{gw.base_url}/v1/scheduler/stats",
                            headers=gw.admin())).json()


def break_provider(fleet, name: str, **faults) -> None:
    fleet.state.profiles[name] = fleet.state.profiles[name].with_faults(FaultSpec(**faults))


# -- failover through the gateway ------------------------------------------


async def test_a_failing_provider_fails_over_and_the_client_sees_a_normal_response(
    fleet_server,
):
    config, keys = routed_config()
    break_provider(fleet_server, "quick", error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        r = await post(gw, "alpha")

    assert r.status_code == 200
    assert r.json()["usage"]["completion_tokens"] > 0
    assert r.headers.get("x-switchyard-provider") == "held"
    assert r.headers.get("x-switchyard-failed-over") == "true"


async def test_a_hung_provider_fails_over_on_the_ttft_deadline(fleet_server):
    """A provider that accepts and never answers must not hold the request open."""
    config, keys = routed_config(timeouts=TimeoutPolicy(ttft_s=0.4, total_s=30.0))
    fleet_server.state.profiles["quick"] = ProviderProfile(
        name="quick", ttft_median_ms=30_000.0, ttft_sigma=0.01,
        output_tokens_median=4.0, output_tokens_sigma=0.01, tokens_per_second=2000.0,
    )

    async with serve_gateway(config, fleet_server, keys) as gw:
        r = await post(gw, "alpha")

    assert r.status_code == 200
    assert r.headers.get("x-switchyard-provider") == "held"


async def test_when_every_provider_fails_the_client_gets_a_typed_terminal_error(
    fleet_server,
):
    config, keys = routed_config()
    for name in ("quick", "held"):
        break_provider(fleet_server, name, error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        r = await post(gw, "alpha")
        after = await scheduler_stats(gw)

    payload = r.json()
    # Nothing was delivered, so the status line says so rather than making the
    # client inspect a 200 body to discover the request failed.
    assert r.status_code == 502
    assert payload["choices"][0]["finish_reason"] == "provider_error"
    assert "all providers" in payload["error"]["message"]
    assert after["inflight"] == 0, "capacity must be released even when everything failed"


async def test_a_mid_stream_failure_is_not_failed_over(fleet_server):
    """Splicing a second answer onto a partial one would corrupt the response."""
    config, keys = routed_config()
    break_provider(fleet_server, "quick", abort_rate=1.0, abort_after_chunks=3)

    async with serve_gateway(config, fleet_server, keys) as gw:
        r = await post(gw, "alpha")

    payload = r.json()
    # Partial content still returns 200: the client has real tokens, and the
    # error frame explains why there are not more -- the same contract the
    # streaming path offers.
    assert r.status_code == 200
    assert payload["choices"][0]["finish_reason"] == "provider_error"
    assert payload["error"]["tokens_emitted"] == 3
    assert r.headers.get("x-switchyard-failed-over") is None


# -- capacity and budget survive failure -----------------------------------


async def test_failover_reuses_one_capacity_slot_rather_than_taking_a_second(
    fleet_server,
):
    """A retry is still one request competing for capacity, not two."""
    config, keys = routed_config()
    break_provider(fleet_server, "quick", error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        peaks = []
        async def watch():
            for _ in range(40):
                peaks.append((await scheduler_stats(gw))["inflight"])
                await asyncio.sleep(0.01)

        watcher = asyncio.create_task(watch())
        await asyncio.gather(*(post(gw, "alpha") for _ in range(4)))
        await watcher
        final = await scheduler_stats(gw)

    assert max(peaks) <= 4, f"more slots held than requests issued: {max(peaks)}"
    assert final["inflight"] == 0


async def test_capacity_is_released_when_a_request_fails_after_acquiring_it(
    fleet_server,
):
    config, keys = routed_config()
    for name in ("quick", "held"):
        break_provider(fleet_server, name, error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        await asyncio.gather(*(post(gw, "alpha") for _ in range(12)))
        await asyncio.sleep(0.1)
        stats = await scheduler_stats(gw)

    assert stats["inflight"] == 0
    assert stats["queue_depth"] == 0
    assert stats["shared_pool"]["in_use"] == 0


async def test_a_failed_request_does_not_consume_budget(fleet_server):
    """No tokens were generated, so nothing should be charged."""
    config, keys = routed_config(tenant={"budget_tokens": 50_000})
    for name in ("quick", "held"):
        break_provider(fleet_server, name, error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        await asyncio.gather(*(post(gw, "alpha") for _ in range(8)))
        await asyncio.sleep(0.1)
        budget = (await scheduler_stats(gw))["tenants"]["alpha"]["budget"]

    assert budget["spent"] == 0, "failed requests generated nothing to charge for"
    assert budget["reserved_in_flight"] == 0, "reservations must be released"


async def test_a_mid_stream_failure_charges_only_what_was_delivered(fleet_server):
    config, keys = routed_config(tenant={"budget_tokens": 50_000})
    break_provider(fleet_server, "quick", abort_rate=1.0, abort_after_chunks=3)

    async with serve_gateway(config, fleet_server, keys) as gw:
        await post(gw, "alpha", BODY | {"max_tokens": 4096})
        await asyncio.sleep(0.1)
        budget = (await scheduler_stats(gw))["tenants"]["alpha"]["budget"]

    assert budget["spent"] == 3, "exactly the tokens the client received"
    assert budget["reserved_in_flight"] == 0


async def test_client_disconnect_during_provider_failure_leaks_nothing(fleet_server):
    """Cancellation and provider failure at once still release both resources."""
    config, keys = routed_config(tenant={"budget_tokens": 50_000})
    break_provider(fleet_server, "quick", abort_rate=1.0, abort_after_chunks=2)

    async with serve_gateway(config, fleet_server, keys) as gw:
        for _ in range(4):
            with pytest.raises((httpx.ReadError, httpx.RemoteProtocolError, StopAsyncIteration)):
                async with (
                    httpx.AsyncClient(timeout=10.0) as c,
                    c.stream("POST", f"{gw.base_url}/v1/chat/completions",
                             json=BODY | {"stream": True, "max_tokens": 4096},
                             headers=gw.auth("alpha")) as r,
                ):
                    it = r.aiter_lines()
                    await anext(it)
                    raise StopAsyncIteration           # walk away mid-stream

        await asyncio.sleep(0.3)
        stats = await scheduler_stats(gw)

    assert stats["inflight"] == 0
    assert stats["tenants"]["alpha"]["budget"]["reserved_in_flight"] == 0


# -- breaker through the gateway -------------------------------------------


async def test_a_persistently_failing_provider_is_taken_out_of_rotation(fleet_server):
    config, keys = routed_config(breaker=BreakerConfig(min_samples=4, cooldown_s=60.0))
    break_provider(fleet_server, "quick", error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        for _ in range(8):
            assert (await post(gw, "alpha")).status_code == 200

        async with httpx.AsyncClient() as c:
            metrics = (await c.get(f"{gw.base_url}/metrics", headers=gw.admin())).text
            health = (await c.get(f"{gw.base_url}/v1/providers", headers=gw.admin())).json()

    assert health["quick"]["state"] == "open", "the failing provider should be shut out"
    assert health["held"]["state"] == "closed"
    assert 'switchyard_breaker_state{provider="quick"} 2.0' in metrics
    assert 'switchyard_failovers_total{from_provider="quick",to_provider="held"}' in metrics


async def test_an_open_breaker_stops_calling_the_failing_provider(fleet_server):
    config, keys = routed_config(breaker=BreakerConfig(min_samples=4, cooldown_s=60.0))
    break_provider(fleet_server, "quick", error_rate=1.0)

    async with serve_gateway(config, fleet_server, keys) as gw:
        for _ in range(8):
            await post(gw, "alpha")
        async with httpx.AsyncClient() as c:
            before = (await c.get(f"{gw.base_url}/metrics", headers=gw.admin())).text
            for _ in range(5):
                await post(gw, "alpha")
            after = (await c.get(f"{gw.base_url}/metrics", headers=gw.admin())).text

    def skipped(text: str) -> float:
        for line in text.splitlines():
            if line.startswith('switchyard_provider_skipped_total{provider="quick"}'):
                return float(line.rsplit(" ", 1)[1])
        return 0.0

    assert skipped(after) >= skipped(before) + 5, "requests should skip the open breaker"


async def test_a_caller_error_does_not_open_the_breaker_for_everyone(fleet_server):
    """One tenant's malformed requests must not cost every tenant the provider."""
    config, keys = routed_config(breaker=BreakerConfig(min_samples=2, cooldown_s=60.0))
    break_provider(fleet_server, "quick", error_rate=1.0,
                   error_class=ErrorClass.BAD_REQUEST)

    async with serve_gateway(config, fleet_server, keys) as gw:
        for _ in range(6):
            await post(gw, "alpha")
        async with httpx.AsyncClient() as c:
            health = (await c.get(f"{gw.base_url}/v1/providers", headers=gw.admin())).json()

    assert health["quick"]["state"] == "closed"


async def test_providers_endpoint_describes_routes_and_health(fleet_server):
    config, keys = routed_config()
    async with serve_gateway(config, fleet_server, keys) as gw, httpx.AsyncClient() as c:
        health = (await c.get(f"{gw.base_url}/v1/providers", headers=gw.admin())).json()
    assert set(health) == {"quick", "held"}
    assert health["quick"]["state"] == "closed"


# -- shutdown --------------------------------------------------------------


async def test_health_reports_503_while_draining_so_load_balancers_back_off(
    fleet_server,
):
    """Readiness, not liveness: the process still works, it just wants no more work."""
    config, keys = routed_config()
    async with (
        serve_gateway(config, fleet_server, keys) as gw,
        httpx.AsyncClient(timeout=5.0) as c,
    ):
        before = await c.get(f"{gw.base_url}/health")
        assert before.status_code == 200 and before.json()["status"] == "ok"

        # Drain in the background: the server keeps serving /health throughout.
        drain = asyncio.create_task(_drain_gateway(gw))
        await asyncio.sleep(0.05)

        during = await c.get(f"{gw.base_url}/health")
        rejected = await post(gw, "alpha")
        await drain

    assert during.status_code == 503
    assert during.json()["status"] == "draining"
    assert rejected.status_code == 503
    assert rejected.json()["error"]["type"] == "shutting_down"


async def _drain_gateway(gw) -> None:
    """Reach into the running app to start a drain without stopping the server."""
    async with httpx.AsyncClient(timeout=5.0) as c:
        await c.post(f"{gw.base_url}/v1/admin/drain", headers=gw.admin())


# -- operational endpoint protection ---------------------------------------


OPERATIONAL = ["/metrics", "/v1/providers", "/v1/scheduler/stats"]


@pytest.mark.parametrize("path", OPERATIONAL)
async def test_operational_endpoints_reject_unauthenticated_callers(path, fleet_server):
    """These carry every tenant's usage, so no tenant may read them."""
    config, keys = routed_config()
    async with serve_gateway(config, fleet_server, keys) as gw, httpx.AsyncClient() as c:
        anonymous = await c.get(f"{gw.base_url}{path}")
        as_tenant = await c.get(f"{gw.base_url}{path}", headers=gw.auth("alpha"))
        as_admin = await c.get(f"{gw.base_url}{path}", headers=gw.admin())

    assert anonymous.status_code == 401
    assert as_tenant.status_code == 401, "a tenant key must not unlock operations"
    assert as_admin.status_code == 200


async def test_drain_cannot_be_triggered_without_the_admin_key(fleet_server):
    """Otherwise one unauthenticated request takes the gateway out of service."""
    config, keys = routed_config()
    async with serve_gateway(config, fleet_server, keys) as gw, httpx.AsyncClient() as c:
        refused = await c.post(f"{gw.base_url}/v1/admin/drain")
        still_serving = await post(gw, "alpha")

    assert refused.status_code == 401
    assert still_serving.status_code == 200, "the refused drain must not have taken effect"


async def test_configuring_tenants_without_an_admin_key_fails_closed(fleet_server):
    """An endpoint that can drain the gateway should not default to open."""
    from dataclasses import replace

    config, keys = routed_config()
    config = replace(config, admin_key_sha256=None)
    async with serve_gateway(config, fleet_server, keys) as gw, httpx.AsyncClient() as c:
        r = await c.get(f"{gw.base_url}/v1/scheduler/stats", headers=gw.admin())

    assert r.status_code == 401
    assert "admin_key_sha256" in r.text, "the error should say how to fix it"


async def test_open_mode_leaves_operational_endpoints_reachable(gateway_server):
    """A fresh checkout with no tenants configured must work without setup."""
    async with httpx.AsyncClient() as c:
        for path in OPERATIONAL:
            assert (await c.get(f"{gateway_server.base_url}{path}")).status_code == 200

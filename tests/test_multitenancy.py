"""Multi-tenancy through the HTTP surface.

The unit tests cover ordering and capacity in isolation. These check that the
pieces are actually wired together: that a request is authenticated, accounted
to the right tenant, queued when the gateway is busy, and refused with a status
a client can act on rather than an error buried inside a 200.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

BODY = {"model": "quick", "messages": [{"role": "user", "content": "hello"}], "max_tokens": 64}
SLOW = BODY | {"model": "held"}


async def post(gw, tenant: str | None, body: dict, **kw) -> httpx.Response:
    headers = gw.auth(tenant) if tenant else {}
    async with httpx.AsyncClient(timeout=30.0) as c:
        return await c.post(f"{gw.base_url}/v1/chat/completions",
                            json=body, headers=headers, **kw)


async def stats(gw) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as c:
        return (await c.get(f"{gw.base_url}/v1/scheduler/stats")).json()


# -- authentication --------------------------------------------------------


async def test_a_request_without_a_key_is_refused(tenant_gateway):
    assert (await post(tenant_gateway, None, BODY)).status_code == 401


async def test_a_bad_key_is_refused(tenant_gateway):
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{tenant_gateway.base_url}/v1/chat/completions",
                         json=BODY, headers={"authorization": "Bearer sk_sy_alpha_wrong"})
    assert r.status_code == 401
    assert "invalid API key" in r.text


async def test_a_valid_key_is_served(tenant_gateway):
    r = await post(tenant_gateway, "alpha", BODY)
    assert r.status_code == 200
    assert r.json()["usage"]["completion_tokens"] > 0


async def test_open_mode_serves_without_a_key(gateway_server):
    """A checkout with no tenants configured must work without setup."""
    async with httpx.AsyncClient(timeout=30.0) as c:
        health = (await c.get(f"{gateway_server.base_url}/health")).json()
        r = await c.post(f"{gateway_server.base_url}/v1/chat/completions", json=BODY)
    assert "disabled" in health["auth"]
    assert r.status_code == 200


# -- capacity and queueing -------------------------------------------------


async def test_work_is_accounted_to_the_authenticated_tenant(tenant_gateway):
    await post(tenant_gateway, "alpha", BODY)
    async with httpx.AsyncClient() as c:
        metrics = (await c.get(f"{tenant_gateway.base_url}/metrics")).text
    assert 'switchyard_tenant_tokens_total{tenant="alpha"}' in metrics
    assert 'switchyard_dispatched_total{tenant="alpha"}' in metrics


async def test_queue_wait_is_reported_to_the_client(tenant_gateway):
    """Capacity is 2, so the third concurrent stream must wait for a slot."""
    async def stream(tenant: str) -> float:
        async with (
            httpx.AsyncClient(timeout=30.0) as c,
            c.stream("POST", f"{tenant_gateway.base_url}/v1/chat/completions",
                     json=SLOW | {"stream": True}, headers=tenant_gateway.auth(tenant)) as r,
        ):
            assert r.status_code == 200
            wait = float(r.headers["x-switchyard-queue-wait-ms"])
            async for _ in r.aiter_lines():
                pass
            return wait

    waits = await asyncio.gather(*(stream("alpha") for _ in range(6)))
    assert max(waits) > 0.0, "with capacity 2 and 6 concurrent streams, some must queue"


async def test_capacity_returns_to_zero_after_a_burst(tenant_gateway):
    await asyncio.gather(*(post(tenant_gateway, "alpha", BODY) for _ in range(12)))
    await asyncio.sleep(0.1)
    snapshot = await stats(tenant_gateway)
    assert snapshot["inflight"] == 0, "capacity leaked"
    assert snapshot["queue_depth"] == 0
    assert snapshot["shared_pool"]["in_use"] == 0


async def test_a_full_queue_returns_429_with_retry_after(fleet_server):
    """Overload must be a status the client can act on, not a hang.

    Capacity 1 and a queue of 2, so the fourth concurrent request has nowhere to
    go. Explicit small limits rather than the shared fixture's: with a large
    queue the requests drain while the test is still filling it, and the
    deadline path fires instead -- also correct behaviour, but not this test.
    """
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config(
        max_concurrency=1, policy="drr", max_queue_depth=2, deadline_s=30.0
    )
    async with serve_gateway(config, fleet_server, keys) as gw:
        held = [asyncio.create_task(post(gw, "alpha", SLOW | {"stream": True}))
                for _ in range(3)]
        await asyncio.sleep(0.15)
        rejected = await post(gw, "alpha", BODY)

        for task in held:
            task.cancel()
        await asyncio.gather(*held, return_exceptions=True)

    assert rejected.status_code == 429
    assert rejected.headers.get("retry-after") == "1"
    assert rejected.json()["error"]["type"] == "queue_full"


async def test_stats_endpoint_describes_the_live_scheduler(tenant_gateway):
    snapshot = await stats(tenant_gateway)
    assert snapshot["policy"] == "drr"
    assert snapshot["max_concurrency"] == 2
    assert set(snapshot["tenants"]) == {"alpha", "beta"}
    assert snapshot["tenants"]["alpha"]["weight"] == 1.0


# -- prediction feeds back -------------------------------------------------


async def test_the_predictor_learns_from_observed_output(tenant_gateway):
    """Cold-start estimates should move toward what the tenant actually uses."""
    before = (await stats(tenant_gateway))["tenants"]["alpha"]["predicted_output_tokens"]["quick"]
    for _ in range(8):
        await post(tenant_gateway, "alpha", BODY)
    after = (await stats(tenant_gateway))["tenants"]["alpha"]["predicted_output_tokens"]["quick"]

    assert before != after, "prediction should update once real output is observed"
    assert after < before, "the fixture emits far fewer tokens than the cold-start prior"


@pytest.mark.parametrize("policy", ["drr", "fifo"])
async def test_both_policies_serve_traffic(policy, fleet_server):
    """The baseline must remain a working configuration, not a dead code path."""
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config(max_concurrency=2, policy=policy, deadline_s=10.0)
    async with serve_gateway(config, fleet_server, keys) as gw:
        responses = await asyncio.gather(*(post(gw, "alpha", BODY) for _ in range(6)))
    assert all(r.status_code == 200 for r in responses)


# -- budget ----------------------------------------------------------------


async def test_budget_drains_and_then_refuses_with_402(fleet_server):
    """Exhaustion is 402, not 429: retrying will never help, so do not suggest it."""
    from tests.conftest import build_tenant_config, serve_gateway

    # The fixture provider emits ~8 tokens per request, so this exhausts in
    # roughly 17 requests rather than needing hundreds.
    config, keys = build_tenant_config(max_concurrency=4, budget_tokens=150)
    async with serve_gateway(config, fleet_server, keys) as gw:
        statuses = []
        for _ in range(60):
            r = await post(gw, "alpha", BODY)
            statuses.append(r.status_code)
            if r.status_code == 402:
                assert r.json()["error"]["type"] == "budget_exhausted"
                assert "retry-after" not in r.headers
                break

        snapshot = (await stats(gw))["tenants"]["alpha"]["budget"]
        # The other tenant shares nothing with the exhausted one.
        other = await post(gw, "beta", BODY)

    assert 200 in statuses and statuses[-1] == 402
    assert snapshot["spent"] <= snapshot["limit"], "spend must never exceed the limit"
    assert snapshot["reserved_in_flight"] == 0
    assert other.status_code == 200, "one tenant's exhaustion must not affect another"


async def test_max_tokens_is_clamped_near_the_limit_and_the_client_is_told(fleet_server):
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config(max_concurrency=4, budget_tokens=300)
    async with serve_gateway(config, fleet_server, keys) as gw:
        clamped = None
        for _ in range(60):
            r = await post(gw, "alpha", BODY | {"max_tokens": 4096, "stream": True})
            if r.status_code == 402:
                break
            if "x-switchyard-max-tokens-clamped" in r.headers:
                clamped = int(r.headers["x-switchyard-max-tokens-clamped"])
                break

    assert clamped is not None, "a request near the limit should have been clamped"
    assert 0 < clamped <= 300


async def test_budget_is_not_consumed_by_a_cancelled_request(fleet_server):
    """A client that walks away pays only for what was actually generated."""
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config(max_concurrency=4, budget_tokens=100_000)
    async with serve_gateway(config, fleet_server, keys) as gw:
        async with (
            httpx.AsyncClient(timeout=30.0) as c,
            c.stream("POST", f"{gw.base_url}/v1/chat/completions",
                     json=SLOW | {"stream": True, "max_tokens": 4096},
                     headers=gw.auth("alpha")) as r,
        ):
            assert r.status_code == 200
            async for line in r.aiter_lines():
                if '"content"' in line:
                    break                       # abandon after one token

        await asyncio.sleep(0.3)
        budget = (await stats(gw))["tenants"]["alpha"]["budget"]

    assert budget["reserved_in_flight"] == 0, "the reservation must be released"
    assert budget["spent"] < 4096, "only generated tokens are charged, not the ceiling"


async def test_stats_reports_budget_position(tenant_gateway):
    snapshot = (await stats(tenant_gateway))["tenants"]["alpha"]["budget"]
    assert snapshot["limit"] is None, "the shared fixture configures no budget"

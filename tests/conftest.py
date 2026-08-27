"""Shared fixtures.

The fleet fixture runs a real uvicorn server on an ephemeral port rather than
using an in-process ASGI transport. Streaming behavior over a real socket is not
the same as in-process iteration -- buffering, chunk boundaries, and connection
teardown all differ -- and those are exactly the things the load generator and
the stream pump are supposed to get right.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest_asyncio
import uvicorn

from switchyard.synthetic.app import FleetState, create_app


class RunningServer:
    def __init__(self, base_url: str, state: FleetState) -> None:
        self.base_url = base_url
        self.state = state


async def _serve(app) -> tuple[uvicorn.Server, asyncio.Task, int]:
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    # uvicorn exposes readiness as a plain bool, not an awaitable event, so this
    # polls. noqa: ASYNC110 -- there is no event to await.
    while not server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    return server, task, port


@pytest_asyncio.fixture
async def fleet_server() -> AsyncIterator[RunningServer]:
    from switchyard.synthetic.profiles import ProviderProfile

    state = FleetState(
        {
            "quick": ProviderProfile(
                name="quick", ttft_median_ms=5.0, ttft_sigma=0.01,
                output_tokens_median=8.0, output_tokens_sigma=0.01,
                tokens_per_second=5000.0, token_jitter_sigma=0.0,
            ),
            "held": ProviderProfile(
                name="held", ttft_median_ms=200.0, ttft_sigma=0.01,
                output_tokens_median=4.0, output_tokens_sigma=0.01,
                tokens_per_second=5000.0, token_jitter_sigma=0.0,
            ),
        },
        run_seed=1,
    )
    server, task, port = await _serve(create_app(state))
    try:
        yield RunningServer(f"http://127.0.0.1:{port}", state)
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)


class RunningGateway:
    def __init__(self, base_url: str, fleet: RunningServer) -> None:
        self.base_url = base_url
        self.fleet = fleet


@pytest_asyncio.fixture
async def gateway_server(fleet_server: RunningServer) -> AsyncIterator[RunningGateway]:
    """Gateway wired to the fleet fixture, both over real sockets."""
    from switchyard.gateway.app import create_app as create_gateway

    server, task, port = await _serve(
        create_gateway(fleet_url=fleet_server.base_url, providers=("quick", "held"))
    )
    try:
        yield RunningGateway(f"http://127.0.0.1:{port}", fleet_server)
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)

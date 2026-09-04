"""Talking to a real, non-synthetic provider.

Switchyard speaks one upstream wire format -- OpenAI chat completions -- so the
synthetic fleet and a production provider run the same adapter rather than the
demo path being a special case. These tests prove that with a stub that speaks
the plain OpenAI shape (`/v1/chat/completions`, not the fleet's per-provider
path) and demands a key, so the real-provider path is exercised without
credentials or spend.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import httpx
import orjson
import pytest
import pytest_asyncio
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse

from switchyard.adapters.openai_compatible import OpenAICompatibleAdapter, build_client
from switchyard.core.config import ConfigError, GatewayConfig, ProviderConfig
from switchyard.types import CompletionRequest, Message, StreamDone, TokenChunk

KEY_ENV = "SWITCHYARD_TEST_UPSTREAM_KEY"
SECRET = "sk-upstream-secret"


def upstream_app(seen: dict) -> FastAPI:
    """A minimal OpenAI-compatible provider that insists on being paid."""
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def completions(body: dict, authorization: str | None = Header(default=None)):
        if authorization != f"Bearer {SECRET}":
            raise HTTPException(401, "missing or wrong api key")
        seen["authorization"] = authorization
        seen["model"] = body.get("model")

        async def frames() -> AsyncIterator[bytes]:
            for word in ("hello ", "there "):
                yield b"data: " + orjson.dumps({
                    "choices": [{"index": 0, "delta": {"content": word},
                                 "finish_reason": None}]
                }) + b"\n\n"
            yield b"data: " + orjson.dumps({
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 3, "completion_tokens": 2},
            }) + b"\n\n"
            yield b"data: [DONE]\n\n"

        return StreamingResponse(frames(), media_type="text/event-stream")

    return app


@pytest_asyncio.fixture
async def upstream():
    """A stub provider on a real socket, plus the calls it received."""
    import uvicorn

    seen: dict = {}
    config = uvicorn.Config(upstream_app(seen), host="127.0.0.1", port=0,
                            log_level="error", access_log=False)
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    while not server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/v1", seen
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)


REQUEST = CompletionRequest(
    model="chat", messages=(Message(role="user", content="hi"),),
    max_tokens=64, temperature=0.0, stream=True, request_id="r-1",
)


# -- configuration ---------------------------------------------------------


def test_an_unconfigured_provider_defaults_to_the_synthetic_fleet():
    """A fresh checkout must work with no credentials and no spend."""
    config = GatewayConfig(fleet_url="http://fleet:8100", providers=("fast",))
    assert config.endpoint_for("fast").base_url == "http://fleet:8100/v1/fast"
    assert config.endpoint_for("fast").api_key_env is None
    assert config.real_providers == ()


def test_a_configured_provider_overrides_the_default():
    config = GatewayConfig(
        providers=("openai",),
        provider_endpoints={"openai": ProviderConfig(
            name="openai", base_url="https://api.openai.com/v1",
            api_key_env="OPENAI_API_KEY", upstream_model="gpt-4o-mini",
        )},
    )
    config.validate()
    endpoint = config.endpoint_for("openai")
    assert endpoint.base_url == "https://api.openai.com/v1"
    assert endpoint.upstream_model == "gpt-4o-mini"
    assert config.real_providers == ("openai",)


def test_a_provider_endpoint_for_an_unlisted_provider_is_rejected():
    config = GatewayConfig(
        providers=("fast",),
        provider_endpoints={"ghost": ProviderConfig(name="ghost", base_url="http://x")},
    )
    with pytest.raises(ConfigError, match="ghost"):
        config.validate()


def test_a_non_http_base_url_is_rejected():
    with pytest.raises(ConfigError, match="http"):
        ProviderConfig(name="p", base_url="api.openai.com").validate()


def test_the_api_key_is_read_from_the_environment_not_the_file():
    """A config carrying a provider credential is one you cannot commit."""
    endpoint = ProviderConfig(name="p", base_url="http://x", api_key_env=KEY_ENV)
    assert endpoint.needs_key and not endpoint.key_available
    os.environ[KEY_ENV] = SECRET
    try:
        assert endpoint.api_key() == SECRET and endpoint.key_available
    finally:
        del os.environ[KEY_ENV]


def test_parsing_a_provider_section(tmp_path):
    from switchyard.core.config import load_config

    path = tmp_path / "c.toml"
    path.write_text(
        '[gateway]\nproviders = ["openai"]\n\n'
        '[providers.openai]\nbase_url = "https://api.openai.com/v1"\n'
        'api_key_env = "OPENAI_API_KEY"\nupstream_model = "gpt-4o-mini"\n'
    )
    config = load_config(path)
    assert config.endpoint_for("openai").api_key_env == "OPENAI_API_KEY"


def test_an_unknown_provider_field_is_rejected_by_name(tmp_path):
    from switchyard.core.config import load_config

    path = tmp_path / "c.toml"
    path.write_text(
        '[gateway]\nproviders = ["openai"]\n\n'
        '[providers.openai]\nbase_url = "https://x/v1"\nkey = "oops"\n'
    )
    with pytest.raises(ConfigError, match="key"):
        load_config(path)


# -- actually calling one --------------------------------------------------


async def test_the_adapter_talks_to_a_plain_openai_endpoint(upstream):
    """Not the fleet's per-provider path: the shape a real provider exposes."""
    base_url, seen = upstream
    os.environ[KEY_ENV] = SECRET
    client = build_client()
    try:
        adapter = OpenAICompatibleAdapter(
            ProviderConfig(name="upstream", base_url=base_url, api_key_env=KEY_ENV),
            client,
        )
        events = [e async for e in adapter.stream(REQUEST)]
    finally:
        await client.aclose()
        del os.environ[KEY_ENV]

    assert [e.text for e in events if isinstance(e, TokenChunk)] == ["hello ", "there "]
    terminal = events[-1]
    assert isinstance(terminal, StreamDone)
    assert terminal.usage.completion_tokens == 2
    assert seen["authorization"] == f"Bearer {SECRET}"


async def test_a_missing_api_key_surfaces_as_a_provider_error(upstream):
    """No key configured means the upstream refuses, and that is a typed failure."""
    base_url, _ = upstream
    client = build_client()
    try:
        adapter = OpenAICompatibleAdapter(
            ProviderConfig(name="upstream", base_url=base_url), client
        )
        events = [e async for e in adapter.stream(REQUEST)]
    finally:
        await client.aclose()

    from switchyard.types import ErrorClass, StreamFailed

    assert isinstance(events[-1], StreamFailed)
    assert events[-1].error_class is ErrorClass.BAD_REQUEST
    assert events[-1].chunks_emitted == 0


async def test_the_upstream_model_name_is_substituted(upstream):
    """A tenant-facing model name can outlive the model behind it."""
    base_url, seen = upstream
    os.environ[KEY_ENV] = SECRET
    client = build_client()
    try:
        adapter = OpenAICompatibleAdapter(
            ProviderConfig(name="u", base_url=base_url, api_key_env=KEY_ENV,
                           upstream_model="gpt-4o-mini"),
            client,
        )
        [e async for e in adapter.stream(REQUEST)]
    finally:
        await client.aclose()
        del os.environ[KEY_ENV]

    assert seen["model"] == "gpt-4o-mini", "tenants asked for 'chat'"


async def test_a_real_provider_is_served_through_the_whole_gateway(upstream, fleet_server):
    """End to end: scheduling, budgets and failover in front of a real endpoint."""
    from dataclasses import replace

    from tests.conftest import build_tenant_config, serve_gateway

    base_url, seen = upstream
    os.environ[KEY_ENV] = SECRET
    config, keys = build_tenant_config(max_concurrency=4)
    config = replace(
        config,
        providers=("upstream",),
        provider_endpoints={"upstream": ProviderConfig(
            name="upstream", base_url=base_url, api_key_env=KEY_ENV
        )},
    )
    config.validate()

    try:
        async with (
            serve_gateway(config, fleet_server, keys, providers=("upstream",)) as gw,
            httpx.AsyncClient() as c,
        ):
            r = await c.post(
                f"{gw.base_url}/v1/chat/completions",
                json={"model": "upstream",
                      "messages": [{"role": "user", "content": "hi"}], "max_tokens": 32},
                headers=gw.auth("alpha"),
            )
            stats = (await c.get(f"{gw.base_url}/v1/scheduler/stats",
                                 headers=gw.admin())).json()
    finally:
        del os.environ[KEY_ENV]

    assert r.status_code == 200
    assert r.json()["choices"][0]["message"]["content"] == "hello there "
    assert r.headers["x-switchyard-provider"] == "upstream"
    assert seen["authorization"] == f"Bearer {SECRET}", "the key reached the provider"
    assert stats["inflight"] == 0, "capacity released against a real provider too"

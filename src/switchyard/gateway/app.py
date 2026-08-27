"""The Switchyard gateway.

Week 1 scope: a deliberately thin passthrough. There is no auth, no rate limit,
no admission control, no cache, and no scheduler yet. That is the point -- the
baseline this produces is the latency floor, the number every later scheduling
change is measured against. Adding features before the measurement rig exists
would leave nothing to compare them to.

Unsupported request fields are rejected with a 400 that names the field rather
than being silently ignored. A client that asked for tool calls and got a plain
completion has been given a wrong answer, not a degraded one.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from switchyard.adapters.synthetic import SyntheticAdapter, build_client
from switchyard.gateway.stream import collect, to_sse
from switchyard.obs.metrics import (
    REGISTRY,
    RequestTimeline,
    monitor_event_loop_lag,
    track_inflight,
)
from switchyard.types import CompletionRequest, Message

# Fields Switchyard knowingly does not implement. Rejected by name so a caller
# never receives a response that quietly ignored part of the request.
UNSUPPORTED_FIELDS = (
    "tools", "functions", "tool_choice", "function_call", "logprobs",
    "top_logprobs", "response_format", "seed", "logit_bias", "stop",
)

MAX_MESSAGES = 64
MAX_PROMPT_CHARS = 100_000
DEFAULT_MAX_TOKENS = 512


def parse_request(body: dict[str, Any], request_id: str) -> CompletionRequest:
    if not isinstance(body, dict):
        raise HTTPException(400, "request body must be a JSON object")

    for field in UNSUPPORTED_FIELDS:
        if body.get(field) is not None:
            raise HTTPException(400, f"unsupported field {field!r}")
    if body.get("n", 1) != 1:
        raise HTTPException(400, "unsupported field 'n': only n=1 is supported")

    model = body.get("model")
    if not isinstance(model, str) or not model:
        raise HTTPException(400, "'model' is required")

    raw_messages = body.get("messages")
    if not isinstance(raw_messages, list) or not raw_messages:
        raise HTTPException(400, "'messages' must be a non-empty array")
    if len(raw_messages) > MAX_MESSAGES:
        raise HTTPException(400, f"too many messages (max {MAX_MESSAGES})")

    messages = []
    total_chars = 0
    for m in raw_messages:
        role, content = m.get("role"), m.get("content")
        if role not in ("system", "user", "assistant"):
            raise HTTPException(400, f"unsupported message role {role!r}")
        if not isinstance(content, str):
            raise HTTPException(400, "message content must be a string")
        total_chars += len(content)
        messages.append(Message(role=role, content=content))

    # Oversized prompts are rejected before any capacity is consumed. A single
    # very large prompt costs far more than an average one, so accepting it and
    # charging it as one request would be a resource-asymmetry hole.
    if total_chars > MAX_PROMPT_CHARS:
        raise HTTPException(400, f"prompt too large ({total_chars} > {MAX_PROMPT_CHARS} chars)")

    # `or` would treat an explicit 0 as absent and silently substitute the
    # default, which is the silent-ignore behavior this validation exists to
    # prevent. Absent and zero must be distinguished.
    raw_max_tokens = body.get("max_tokens")
    max_tokens = DEFAULT_MAX_TOKENS if raw_max_tokens is None else int(raw_max_tokens)
    if max_tokens < 1:
        raise HTTPException(400, "'max_tokens' must be >= 1")

    temperature = float(body.get("temperature", 0.0))
    if not 0.0 <= temperature <= 2.0:
        raise HTTPException(400, "'temperature' must be in [0, 2]")

    return CompletionRequest(
        model=model,
        messages=tuple(messages),
        max_tokens=max_tokens,
        temperature=temperature,
        stream=bool(body.get("stream", False)),
        request_id=request_id,
    )


def create_app(
    fleet_url: str | None = None, providers: tuple[str, ...] = ("fast", "slow")
) -> FastAPI:
    fleet_url = fleet_url or os.environ.get("SWITCHYARD_FLEET_URL", "http://127.0.0.1:8100")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = build_client()
        app.state.client = client
        app.state.adapters = {
            name: SyntheticAdapter(name, fleet_url, client) for name in providers
        }
        lag_task = asyncio.create_task(monitor_event_loop_lag())
        try:
            yield
        finally:
            lag_task.cancel()
            await client.aclose()

    app = FastAPI(title="Switchyard", lifespan=lifespan)

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        # Started before parsing and routing so that every span the observability
        # model reports is measured from the moment the request landed. Once
        # admission control exists, its queue wait is recorded onto this object.
        timeline = RequestTimeline()
        request_id = request.headers.get("x-switchyard-request-id") or f"sy-{uuid.uuid4().hex[:12]}"
        parsed = parse_request(await request.json(), request_id)

        # Week 1 routing is static: the model name selects the provider. Policy
        # routing arrives with the router; keeping it trivial here means the
        # baseline measures transport and pump cost, nothing else.
        adapter = request.app.state.adapters.get(parsed.model)
        if adapter is None:
            raise HTTPException(404, f"unknown model {parsed.model!r}")

        if parsed.stream:
            async def body_iter():
                async with track_inflight(adapter.name):
                    async for frame in to_sse(
                        adapter.stream(parsed), parsed, adapter.name, timeline
                    ):
                        yield frame

            return StreamingResponse(
                body_iter(),
                media_type="text/event-stream",
                headers={
                    "cache-control": "no-cache",
                    # Without this, an intermediate proxy buffers the whole
                    # response and the client sees one chunk at the end --
                    # streaming that works in dev and silently is not streaming
                    # in a container.
                    "x-accel-buffering": "no",
                },
            )

        async with track_inflight(adapter.name):
            payload = await collect(adapter.stream(parsed), parsed, adapter.name, timeline)
        return JSONResponse(payload)

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()

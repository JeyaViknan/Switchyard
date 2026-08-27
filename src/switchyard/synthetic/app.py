"""The synthetic provider fleet: an HTTP service that imitates LLM providers.

It speaks the OpenAI streaming chat-completions shape and nothing else. The
gateway always calls providers with `stream=true` -- non-streaming responses are
assembled from the stream so there is one code path to reason about -- so this
service implements only the streaming path and rejects the rest loudly rather
than pretending to support it.

One chunk == one output token. That is a simplification, and the right one here:
the experiments care about how many units of work a request costs and how they
are spaced in time, not about tokenizer fidelity.

Fault injection is driven at runtime through `/control/*` so a benchmark can
change provider behavior mid-run without a restart, which is what makes a
recovery-timeline measurement possible.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import replace
from typing import Any

import orjson
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse

from switchyard.synthetic.profiles import (
    DEFAULT_FLEET,
    ProviderProfile,
    RequestPlan,
    plan_request,
)
from switchyard.types import ErrorClass

REQUEST_ID_HEADER = "x-switchyard-request-id"

# Filler vocabulary. Content is irrelevant to scheduling behavior; these are
# short so that a chunk is roughly token-sized to a real tokenizer.
_WORDS = (
    "the", "system", "will", "route", "each", "request", "through", "a",
    "queue", "and", "then", "emit", "tokens", "back", "to", "the", "client",
)

_STATUS_FOR_ERROR = {
    ErrorClass.RATE_LIMITED: 429,
    ErrorClass.SERVER_ERROR: 500,
    ErrorClass.BAD_REQUEST: 400,
}


class AbortStream(Exception):
    """Drop the connection without a terminal frame.

    Models a provider dying mid-response: the client sees bytes stop with no
    `finish_reason` and no `[DONE]`. That ambiguity is the whole point -- it is
    what the gateway must detect and convert into an explicit failure.
    """


class FleetState:
    """Mutable fleet configuration.

    A plain object rather than module globals so tests can build an isolated
    fleet without leaking state between cases.
    """

    def __init__(
        self, profiles: dict[str, ProviderProfile] | None = None, run_seed: int = 1
    ) -> None:
        self._defaults = dict(profiles or DEFAULT_FLEET)
        self.profiles: dict[str, ProviderProfile] = dict(self._defaults)
        self.run_seed = run_seed

    def get(self, name: str) -> ProviderProfile:
        try:
            return self.profiles[name]
        except KeyError:
            raise HTTPException(404, f"unknown provider {name!r}") from None

    def reset(self) -> None:
        self.profiles = dict(self._defaults)


def _sse(payload: dict[str, Any]) -> bytes:
    return b"data: " + orjson.dumps(payload) + b"\n\n"


def _frame(
    request_id: str, model: str, created: int, delta: dict[str, Any],
    finish_reason: str | None = None, usage: dict[str, int] | None = None,
) -> dict[str, Any]:
    frame: dict[str, Any] = {
        "id": request_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    if usage is not None:
        frame["usage"] = usage
    return frame


async def generate(
    plan: RequestPlan, request_id: str, model: str, max_tokens: int, prompt_tokens: int
) -> AsyncIterator[bytes]:
    """Emit the stream described by `plan`.

    Takes a fully-drawn plan rather than an RNG so that no randomness happens
    while the stream is in flight; the behavior of a request is fixed before its
    first byte.
    """
    created = int(time.time())
    await asyncio.sleep(plan.ttft_s)

    emitted = 0
    for i in range(plan.n_tokens):
        if plan.abort_at is not None and emitted >= plan.abort_at:
            raise AbortStream
        if plan.stall_at is not None and emitted == plan.stall_at:
            await asyncio.sleep(plan.stall_s)

        yield _sse(_frame(request_id, model, created, {"content": _WORDS[i % len(_WORDS)] + " "}))
        emitted += 1

        if i < len(plan.inter_token_s):
            await asyncio.sleep(plan.inter_token_s[i])

    yield _sse(
        _frame(
            request_id, model, created, {},
            finish_reason="length" if emitted >= max_tokens else "stop",
            usage={
                "prompt_tokens": prompt_tokens,
                "completion_tokens": emitted,
                "total_tokens": prompt_tokens + emitted,
            },
        )
    )
    yield b"data: [DONE]\n\n"


def create_app(state: FleetState | None = None) -> FastAPI:
    fleet = state or FleetState()
    app = FastAPI(title="Switchyard synthetic provider fleet")
    app.state.fleet = fleet

    @app.post("/v1/{provider}/chat/completions")
    async def chat_completions(provider: str, request: Request) -> Response:
        profile = fleet.get(provider)
        body = await request.json()

        if not body.get("stream", False):
            raise HTTPException(
                400,
                "synthetic fleet is streaming-only; the gateway assembles "
                "non-streaming responses from the stream",
            )

        request_id = request.headers.get(REQUEST_ID_HEADER) or f"syn-{uuid.uuid4().hex[:12]}"
        max_tokens = int(body.get("max_tokens", 4096))
        model = body.get("model", provider)
        prompt_chars = sum(len(m.get("content", "")) for m in body.get("messages", []))
        prompt_tokens = max(1, int(prompt_chars * profile.prompt_tokens_per_char))

        plan = plan_request(profile, fleet.run_seed, request_id, max_tokens)

        # Pre-first-token failures surface as a real HTTP status, as a provider
        # would produce. This is the failure class that can be failed over
        # transparently, so it must be distinguishable from a mid-stream abort.
        if plan.fail_before_first_token:
            status = _STATUS_FOR_ERROR.get(plan.error_class, 500)
            raise HTTPException(status, f"injected {plan.error_class.value}")

        async def body_iter() -> AsyncIterator[bytes]:
            try:
                async for frame in generate(plan, request_id, model, max_tokens, prompt_tokens):
                    yield frame
            except AbortStream:
                return  # connection closes with no terminal frame, by design

        return StreamingResponse(
            body_iter(),
            media_type="text/event-stream",
            headers={"cache-control": "no-cache", "x-accel-buffering": "no"},
        )

    @app.get("/control/profiles")
    async def get_profiles() -> dict[str, Any]:
        return {
            "run_seed": fleet.run_seed,
            "profiles": {n: profile_json(p) for n, p in fleet.profiles.items()},
        }

    @app.put("/control/profiles/{provider}")
    async def put_profile(provider: str, patch: dict[str, Any]) -> dict[str, Any]:
        updated = apply_patch(fleet.get(provider), dict(patch))
        updated.validate()
        fleet.profiles[provider] = updated
        return profile_json(updated)

    @app.put("/control/seed")
    async def put_seed(payload: dict[str, int]) -> dict[str, int]:
        fleet.run_seed = int(payload["run_seed"])
        return {"run_seed": fleet.run_seed}

    @app.post("/control/reset")
    async def reset() -> dict[str, str]:
        fleet.reset()
        return {"status": "reset"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def profile_json(p: ProviderProfile) -> dict[str, Any]:
    return {
        "name": p.name,
        "ttft_median_ms": p.ttft_median_ms,
        "ttft_sigma": p.ttft_sigma,
        "output_tokens_median": p.output_tokens_median,
        "output_tokens_sigma": p.output_tokens_sigma,
        "tokens_per_second": p.tokens_per_second,
        "token_jitter_sigma": p.token_jitter_sigma,
        "faults": {
            "error_rate": p.faults.error_rate,
            "error_class": p.faults.error_class.value,
            "abort_rate": p.faults.abort_rate,
            "abort_after_chunks": p.faults.abort_after_chunks,
            "stall_rate": p.faults.stall_rate,
            "stall_after_chunks": p.faults.stall_after_chunks,
            "stall_seconds": p.faults.stall_seconds,
        },
    }


def apply_patch(profile: ProviderProfile, patch: dict[str, Any]) -> ProviderProfile:
    """Apply a partial profile update. Unknown keys raise rather than being ignored."""
    fault_patch = patch.pop("faults", None)
    updated = replace(profile, **patch) if patch else profile
    if fault_patch:
        if "error_class" in fault_patch:
            fault_patch["error_class"] = ErrorClass(fault_patch["error_class"])
        updated = updated.with_faults(replace(updated.faults, **fault_patch))
    return updated


app = create_app()

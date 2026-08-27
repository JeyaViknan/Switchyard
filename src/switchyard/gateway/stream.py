"""Serialization of normalized stream events back to OpenAI-shaped SSE.

The pump lives here. In week 1 it is deliberately thin -- it forwards events and
guarantees a terminal frame. Cancellation propagation, bounded buffering, and
slow-consumer eviction are the reliability phase's work; the structure below is
shaped to receive them without being rewritten.

Terminal frames
---------------
Every stream ends with either a `finish_reason` frame followed by `[DONE]`, or an
error frame carrying `tokens_emitted`. A stream is never allowed to simply stop:
a client must always be able to tell a complete response from an interrupted one.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator

import orjson

from switchyard.obs.metrics import StreamTimer
from switchyard.types import (
    CompletionRequest,
    StreamDone,
    StreamEvent,
    StreamFailed,
    TokenChunk,
)


def _frame(request: CompletionRequest, created: int, delta: dict, **extra) -> bytes:
    payload = {
        "id": request.request_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": request.model,
        "choices": [
            {"index": 0, "delta": delta, "finish_reason": extra.pop("finish_reason", None)}
        ],
        **extra,
    }
    return b"data: " + orjson.dumps(payload) + b"\n\n"


async def to_sse(
    events: AsyncIterator[StreamEvent], request: CompletionRequest, provider: str
) -> AsyncIterator[bytes]:
    """Forward normalized events to the client as SSE, always terminating cleanly."""
    created = int(time.time())
    timer = StreamTimer()
    outcome = "error"

    try:
        async for event in events:
            if isinstance(event, TokenChunk):
                timer.on_token(provider)
                yield _frame(request, created, {"content": event.text})

            elif isinstance(event, StreamDone):
                outcome = "ok"
                yield _frame(
                    request, created, {},
                    finish_reason=event.finish_reason.value,
                    usage={
                        "prompt_tokens": event.usage.prompt_tokens,
                        "completion_tokens": event.usage.completion_tokens,
                        "total_tokens": event.usage.total_tokens,
                    },
                )
                yield b"data: [DONE]\n\n"

            elif isinstance(event, StreamFailed):
                outcome = f"error_{event.error_class.value}"
                # An explicit, typed terminal frame rather than a dropped
                # connection. `tokens_emitted` tells the client exactly how much
                # of the response it actually received.
                yield _frame(
                    request, created, {},
                    finish_reason="provider_error",
                    error={
                        "type": event.error_class.value,
                        "message": event.message,
                        "tokens_emitted": event.chunks_emitted,
                        "request_id": request.request_id,
                    },
                )
                yield b"data: [DONE]\n\n"
    finally:
        timer.finish(request.model, provider, outcome)


async def collect(
    events: AsyncIterator[StreamEvent], request: CompletionRequest, provider: str
) -> dict:
    """Assemble a non-streaming response from the same stream.

    There is exactly one path to a provider. A separate non-streaming
    implementation would double the failure modes to reason about and test, for
    a response shape the client could have assembled itself.
    """
    created = int(time.time())
    timer = StreamTimer()
    parts: list[str] = []
    terminal: StreamDone | StreamFailed | None = None

    try:
        async for event in events:
            if isinstance(event, TokenChunk):
                timer.on_token(provider)
                parts.append(event.text)
            else:
                terminal = event
    finally:
        outcome = "ok" if isinstance(terminal, StreamDone) else "error"
        timer.finish(request.model, provider, outcome)

    text = "".join(parts)
    if isinstance(terminal, StreamDone):
        usage = terminal.usage
        finish_reason = terminal.finish_reason.value
        error = None
    else:
        usage = terminal.usage if terminal and terminal.usage else None
        finish_reason = "provider_error"
        error = {
            "type": terminal.error_class.value if terminal else "unknown",
            "message": terminal.message if terminal else "stream produced no terminal event",
            "tokens_emitted": terminal.chunks_emitted if terminal else len(parts),
        }

    response = {
        "id": request.request_id,
        "object": "chat.completion",
        "created": created,
        "model": request.model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": usage.prompt_tokens if usage else 0,
            "completion_tokens": usage.completion_tokens if usage else len(parts),
            "total_tokens": usage.total_tokens if usage else len(parts),
        },
    }
    if error:
        response["error"] = error
    return response

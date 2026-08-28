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

from switchyard.obs.metrics import RequestTimeline
from switchyard.types import (
    CompletionRequest,
    FinishReason,
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
    events: AsyncIterator[StreamEvent], request: CompletionRequest,
    timeline: RequestTimeline,
) -> AsyncIterator[bytes]:
    """Forward normalized events to the client as SSE, always terminating cleanly.

    The timeline is created at request arrival and passed in rather than started
    here, so that everything before dispatch -- and, once admission control
    exists, queue wait -- is inside the measured span.
    """
    created = int(time.time())
    outcome = "error"
    timeline.mark_dispatched()

    try:
        async for event in events:
            if isinstance(event, TokenChunk):
                timeline.on_token(timeline.provider)
                yield _frame(request, created, {"content": event.text})

            elif isinstance(event, StreamDone):
                outcome = "ok"
                timeline.mark_provider_done()
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
                timeline.mark_provider_done()
                # An explicit, typed terminal frame rather than a dropped
                # connection. `tokens_emitted` tells the client exactly how much
                # of the response it actually received.
                yield _frame(
                    request, created, {},
                    finish_reason=FinishReason.PROVIDER_ERROR.value,
                    error={
                        "type": event.error_class.value,
                        "message": event.message,
                        "tokens_emitted": event.chunks_emitted,
                        "request_id": request.request_id,
                    },
                )
                yield b"data: [DONE]\n\n"
    finally:
        timeline.finish(request.model, timeline.provider, outcome)


async def collect(
    events: AsyncIterator[StreamEvent], request: CompletionRequest,
    timeline: RequestTimeline,
) -> dict:
    """Assemble a non-streaming response from the same stream.

    There is exactly one path to a provider. A separate non-streaming
    implementation would double the failure modes to reason about and test, for
    a response shape the client could have assembled itself.
    """
    created = int(time.time())
    parts: list[str] = []
    terminal: StreamDone | StreamFailed | None = None
    timeline.mark_dispatched()

    try:
        async for event in events:
            if isinstance(event, TokenChunk):
                timeline.on_token(timeline.provider)
                parts.append(event.text)
            else:
                terminal = event
                timeline.mark_provider_done()
    finally:
        outcome = "ok" if isinstance(terminal, StreamDone) else "error"
        timeline.finish(request.model, timeline.provider, outcome)

    text = "".join(parts)
    if isinstance(terminal, StreamDone):
        usage = terminal.usage
        finish_reason = terminal.finish_reason.value
        error = None
    else:
        usage = terminal.usage if terminal and terminal.usage else None
        finish_reason = FinishReason.PROVIDER_ERROR.value
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

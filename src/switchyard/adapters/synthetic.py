"""Adapter for the synthetic provider fleet.

Adapters translate a provider's wire format into the normalized `StreamEvent`
union. The contract they must honor: never raise for a provider-side failure,
always yield exactly one terminal event. Raising would let a provider outage
propagate as an unhandled exception through the pump, where the client would see
a connection drop with no explanation -- the ambiguous-truncation failure mode.

Connection pool sizing
----------------------
`max_connections` is set well above any concurrency limit the gateway itself
imposes. If httpx's pool were the smaller of the two, requests would queue
*inside httpx*, invisible to the scheduler -- which would then be measuring and
fairly allocating a resource it does not actually control. The scheduler must
always be the binding constraint.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx
import orjson

from switchyard.types import (
    CompletionRequest,
    ErrorClass,
    FinishReason,
    StreamDone,
    StreamEvent,
    StreamFailed,
    TokenChunk,
    Usage,
)

REQUEST_ID_HEADER = "x-switchyard-request-id"

_FINISH_REASONS = {
    "stop": FinishReason.STOP,
    "length": FinishReason.LENGTH,
}


def build_client(
    max_connections: int = 1000, connect_timeout: float = 2.0, total_timeout: float = 300.0
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        limits=httpx.Limits(
            max_connections=max_connections, max_keepalive_connections=max_connections
        ),
        timeout=httpx.Timeout(
            connect=connect_timeout, read=total_timeout, write=10.0, pool=5.0
        ),
    )


def classify_status(status: int) -> ErrorClass:
    if status == 429:
        return ErrorClass.RATE_LIMITED
    if status >= 500:
        return ErrorClass.SERVER_ERROR
    return ErrorClass.BAD_REQUEST


class SyntheticAdapter:
    """Speaks to one named provider in the synthetic fleet."""

    def __init__(self, name: str, base_url: str, client: httpx.AsyncClient) -> None:
        self.name = name
        self._url = f"{base_url.rstrip('/')}/v1/{name}/chat/completions"
        self._client = client

    async def stream(self, request: CompletionRequest) -> AsyncIterator[StreamEvent]:
        body = {
            "model": request.model,
            "messages": [{"role": m.role, "content": m.content} for m in request.messages],
            "stream": True,
            "max_tokens": request.max_tokens,
            "temperature": request.temperature,
        }
        headers = {REQUEST_ID_HEADER: request.request_id}

        emitted = 0
        prompt_tokens = 0
        completion_tokens = 0
        finish: FinishReason | None = None

        try:
            async with self._client.stream(
                "POST", self._url, json=body, headers=headers
            ) as response:
                if response.status_code != 200:
                    await response.aread()
                    yield StreamFailed(
                        error_class=classify_status(response.status_code),
                        message=f"provider returned {response.status_code}",
                        chunks_emitted=0,
                    )
                    return

                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    payload = line[6:]
                    if payload == "[DONE]":
                        yield StreamDone(
                            finish_reason=finish or FinishReason.STOP,
                            usage=Usage(prompt_tokens, completion_tokens or emitted),
                        )
                        return

                    frame = orjson.loads(payload)
                    choice = (frame.get("choices") or [{}])[0]
                    if content := choice.get("delta", {}).get("content"):
                        yield TokenChunk(text=content, index=emitted)
                        emitted += 1
                    if reason := choice.get("finish_reason"):
                        finish = _FINISH_REASONS.get(reason, FinishReason.STOP)
                    if usage := frame.get("usage"):
                        prompt_tokens = usage.get("prompt_tokens", 0)
                        completion_tokens = usage.get("completion_tokens", 0)

        except httpx.ConnectError as exc:
            yield StreamFailed(ErrorClass.CONNECT, str(exc), emitted)
            return
        except httpx.ReadTimeout as exc:
            # Week 1 has a single total read timeout. Separate TTFT and
            # inter-token deadlines arrive with the reliability work; until then
            # a stall and a slow response are not distinguishable here.
            yield StreamFailed(
                ErrorClass.TIMEOUT_TTFT if emitted == 0 else ErrorClass.TIMEOUT_TOKEN,
                str(exc), emitted,
            )
            return
        except httpx.HTTPError as exc:
            yield StreamFailed(ErrorClass.DISCONNECTED, str(exc), emitted)
            return

        # The stream ended without [DONE]. This is the ambiguous case invariant
        # I8 exists for: it must never be reported as a clean completion.
        yield StreamFailed(
            error_class=ErrorClass.DISCONNECTED,
            message="stream ended without a terminal frame",
            chunks_emitted=emitted,
            usage=Usage(prompt_tokens, completion_tokens or emitted),
        )

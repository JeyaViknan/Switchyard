"""Adapter for the synthetic provider fleet.

Adapters translate a provider's wire format into the normalized `StreamEvent`
union. The contract they must honour: never raise for a provider-side failure,
always yield exactly one terminal event. Raising would let a provider outage
propagate as an unhandled exception through the pump, where the client would see
a connection drop with no explanation.

Timeout decomposition
---------------------
Four deadlines, each producing a distinct error class, because "slow" is not one
failure mode:

  connect      -- could not reach the provider at all
  ttft         -- connected, but no first token
  inter-token  -- was streaming, then stalled
  total        -- the whole response took too long

A single overall timeout cannot tell a provider that never answered from one
that answered and then stalled, and it makes both wait its full duration before
anything notices. The per-chunk deadlines wrap only the await on the next chunk,
never the yield to the consumer, so a slow *client* cannot be mistaken for a
slow provider.

Connection pool sizing
----------------------
`max_connections` is set well above any concurrency limit the gateway imposes.
If httpx's pool were the smaller of the two, requests would queue *inside httpx*,
invisible to the scheduler -- which would then be fairly allocating a resource it
does not actually control.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import orjson

from switchyard.core.config import TimeoutPolicy
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

_FINISH_REASONS = {"stop": FinishReason.STOP, "length": FinishReason.LENGTH}


def build_client(
    max_connections: int = 1000, timeouts: TimeoutPolicy | None = None
) -> httpx.AsyncClient:
    policy = timeouts or TimeoutPolicy()
    return httpx.AsyncClient(
        limits=httpx.Limits(
            max_connections=max_connections, max_keepalive_connections=max_connections
        ),
        # Read is left generous: the meaningful read deadlines are TTFT and
        # inter-token, enforced per chunk below where they can be told apart.
        timeout=httpx.Timeout(
            connect=policy.connect_s, read=policy.total_s, write=10.0, pool=5.0
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

    def __init__(
        self, name: str, base_url: str, client: httpx.AsyncClient,
        timeouts: TimeoutPolicy | None = None,
    ) -> None:
        self.name = name
        self._url = f"{base_url.rstrip('/')}/v1/{name}/chat/completions"
        self._client = client
        self._timeouts = timeouts or TimeoutPolicy()

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
            async with asyncio.timeout(self._timeouts.total_s):
                async with self._client.stream(
                    "POST", self._url, json=body, headers=headers
                ) as response:
                    if response.status_code != 200:
                        await response.aread()
                        yield StreamFailed(
                            classify_status(response.status_code),
                            f"provider returned {response.status_code}", 0,
                        )
                        return

                    lines = response.aiter_lines().__aiter__()
                    while True:
                        # Before the first token this is the TTFT deadline;
                        # after it, the inter-token deadline. Only the await is
                        # wrapped, so time the consumer spends is not counted.
                        deadline = (
                            self._timeouts.ttft_s if emitted == 0
                            else self._timeouts.inter_token_s
                        )
                        try:
                            async with asyncio.timeout(deadline):
                                line = await anext(lines)
                        except StopAsyncIteration:
                            break
                        except TimeoutError:
                            yield StreamFailed(
                                ErrorClass.TIMEOUT_TTFT if emitted == 0
                                else ErrorClass.TIMEOUT_TOKEN,
                                f"provider produced no "
                                f"{'first token' if emitted == 0 else 'further tokens'} "
                                f"within {deadline:g}s",
                                emitted,
                            )
                            return

                        if not line.startswith("data: "):
                            continue
                        payload = line[6:]
                        if payload == "[DONE]":
                            yield StreamDone(
                                finish or FinishReason.STOP,
                                Usage(prompt_tokens, completion_tokens or emitted),
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

        except TimeoutError:
            # The inner per-chunk deadlines are caught above, so reaching here
            # means the overall lifetime ran out.
            yield StreamFailed(
                ErrorClass.TIMEOUT_TOTAL,
                f"request exceeded the total deadline of {self._timeouts.total_s:g}s",
                emitted,
            )
            return
        except httpx.ConnectError as exc:
            yield StreamFailed(ErrorClass.CONNECT, str(exc), emitted)
            return
        except httpx.ConnectTimeout as exc:
            yield StreamFailed(ErrorClass.CONNECT, f"connect timed out: {exc}", emitted)
            return
        except httpx.HTTPError as exc:
            yield StreamFailed(ErrorClass.DISCONNECTED, str(exc), emitted)
            return

        # The stream ended without [DONE]: truncated, not complete. Reporting it
        # as a clean completion would make a corrupted response indistinguishable
        # from a correct one.
        yield StreamFailed(
            ErrorClass.DISCONNECTED,
            "stream ended without a terminal frame",
            emitted,
            Usage(prompt_tokens, completion_tokens or emitted),
        )

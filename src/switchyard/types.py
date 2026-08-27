"""Normalized types shared by the gateway and every provider adapter.

Adapters translate provider-specific wire formats into the `StreamEvent` union
below; the gateway serializes that union back out to the OpenAI SSE format.
Nothing above the adapter layer should ever see a provider's native shape.

The terminal-event union is deliberately explicit rather than "the generator
just stops". A stream that ends without a terminal event is indistinguishable
from a truncated one, which is the failure mode invariant I8 exists to prevent.
Week 1 only produces `StreamDone`, but the shape is fixed now so the mid-stream
failure work does not require reshaping the adapter contract.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Literal, Protocol


class FinishReason(enum.StrEnum):
    """Why a stream ended.

    `STOP` and `LENGTH` mirror OpenAI. The remainder are Switchyard-specific and
    exist so a client can tell a complete response from an interrupted one.
    """

    STOP = "stop"
    LENGTH = "length"
    PROVIDER_ERROR = "provider_error"


class ErrorClass(enum.StrEnum):
    """Provider failure taxonomy.

    The distinction that matters is retryable vs not: retryable failures may be
    failed over to another candidate, non-retryable ones must surface to the
    client immediately. Circuit-breaker state is keyed on (provider, class) so a
    tenant-specific failure such as BAD_REQUEST cannot trip the breaker for
    everyone.
    """

    CONNECT = "connect"              # could not establish a connection
    TIMEOUT_TTFT = "timeout_ttft"    # connected, no first token in time
    TIMEOUT_TOKEN = "timeout_token"  # stream stalled mid-flight
    RATE_LIMITED = "rate_limited"    # 429
    SERVER_ERROR = "server_error"    # 5xx
    BAD_REQUEST = "bad_request"      # 4xx other than 429
    DISCONNECTED = "disconnected"    # stream ended without a terminal frame

    @property
    def retryable(self) -> bool:
        return self not in (ErrorClass.BAD_REQUEST,)


@dataclass(frozen=True, slots=True)
class Usage:
    prompt_tokens: int
    completion_tokens: int

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


@dataclass(frozen=True, slots=True)
class TokenChunk:
    """One incremental piece of generated text.

    `index` is the 0-based ordinal of this chunk within the stream. It exists so
    the pump can report exactly how much reached the client when a stream fails
    partway, without recounting.
    """

    text: str
    index: int


@dataclass(frozen=True, slots=True)
class StreamDone:
    """Terminal event for a stream the provider completed."""

    finish_reason: FinishReason
    usage: Usage


@dataclass(frozen=True, slots=True)
class StreamFailed:
    """Terminal event for a stream that ended without the provider completing it.

    `chunks_emitted` is load-bearing: it is what distinguishes a failure that can
    still be transparently failed over (0 chunks reached the client) from one
    that cannot (>0). See the phased-failover policy.
    """

    error_class: ErrorClass
    message: str
    chunks_emitted: int
    usage: Usage | None = None


StreamEvent = TokenChunk | StreamDone | StreamFailed
TerminalEvent = StreamDone | StreamFailed

Role = Literal["system", "user", "assistant"]


@dataclass(frozen=True, slots=True)
class Message:
    role: Role
    content: str


@dataclass(frozen=True, slots=True)
class CompletionRequest:
    """A validated, provider-agnostic completion request.

    This is the internal form. The HTTP layer parses and validates the public
    OpenAI-shaped payload into this; adapters translate it back out. Fields the
    project does not support are rejected at the edge rather than silently
    dropped, so a client never gets a response that quietly ignored what it asked
    for.
    """

    model: str
    messages: tuple[Message, ...]
    max_tokens: int
    temperature: float
    stream: bool
    request_id: str


class ProviderAdapter(Protocol):
    """Contract every provider implementation satisfies.

    Deliberately narrow: one streaming method. Non-streaming responses are
    assembled by the gateway from the same stream, so there is exactly one code
    path to reason about, test, and instrument. A second non-streaming path would
    be a second set of failure modes for no benefit.
    """

    name: str

    def stream(self, request: CompletionRequest): # -> AsyncIterator[StreamEvent]
        """Yield `TokenChunk`s followed by exactly one terminal event.

        Implementations must not raise for provider-side failures; they must
        yield `StreamFailed`. Raising is reserved for programming errors.
        """
        ...

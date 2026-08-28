"""Provider routing and failover semantics.

Uses scripted adapters rather than the synthetic fleet: the question here is
what the router does with a given sequence of events, and scripting them
directly makes each failure mode exact instead of coaxed out of a distribution.
"""

from __future__ import annotations

import pytest

from switchyard.core.health import BreakerPolicy, BreakerState, HealthRegistry
from switchyard.core.routing import ProviderRouter, RouteObserver
from switchyard.types import (
    CompletionRequest,
    ErrorClass,
    FinishReason,
    Message,
    StreamDone,
    StreamFailed,
    TokenChunk,
    Usage,
)

REQUEST = CompletionRequest(
    model="chat", messages=(Message(role="user", content="hi"),),
    max_tokens=256, temperature=0.0, stream=True, request_id="r-1",
)


def tokens(n: int) -> list[TokenChunk]:
    return [TokenChunk(text=f"t{i} ", index=i) for i in range(n)]


def done(n: int = 3) -> StreamDone:
    return StreamDone(FinishReason.STOP, Usage(prompt_tokens=5, completion_tokens=n))


def failed(cls: ErrorClass, emitted: int = 0) -> StreamFailed:
    return StreamFailed(cls, f"injected {cls.value}", emitted)


class ScriptedAdapter:
    """Yields a fixed sequence, recording how many times it was called."""

    def __init__(self, name: str, script: list) -> None:
        self.name = name
        self._script = script
        self.calls = 0

    async def stream(self, request):
        self.calls += 1
        for event in self._script:
            yield event


class Recorder(RouteObserver):
    def __init__(self) -> None:
        self.failovers: list[tuple[str, str, str]] = []
        self.skipped: list[str] = []
        self.terminal: list[tuple[str, str, bool]] = []
        self.successes: list[tuple[str, int]] = []

    def on_failover(self, request_id, frm, to, reason):
        self.failovers.append((frm, to, reason.value))

    def on_skipped(self, request_id, provider):
        self.skipped.append(provider)

    def on_terminal_failure(self, provider, error_class, mid_stream):
        self.terminal.append((provider, error_class.value, mid_stream))

    def on_success(self, provider, attempts):
        self.successes.append((provider, attempts))


def router(adapters, route=("a", "b"), policy=None, observer=None):
    registry = HealthRegistry(tuple(adapters), policy or BreakerPolicy(min_samples=2))
    return ProviderRouter(adapters, {"chat": route}, registry, observer), registry


async def run(rtr) -> list:
    return [event async for event in rtr.stream(REQUEST)]


# -- the happy path --------------------------------------------------------


async def test_a_healthy_provider_serves_without_failover():
    a = ScriptedAdapter("a", [*tokens(3), done()])
    rec = Recorder()
    rtr, _ = router({"a": a, "b": ScriptedAdapter("b", [])}, observer=rec)

    events = await run(rtr)
    assert isinstance(events[-1], StreamDone)
    assert rec.successes == [("a", 1)]
    assert rec.failovers == []


# -- failover before the first token ---------------------------------------


@pytest.mark.parametrize(
    "error_class",
    [ErrorClass.CONNECT, ErrorClass.SERVER_ERROR, ErrorClass.RATE_LIMITED,
     ErrorClass.TIMEOUT_TTFT],
)
async def test_a_failure_before_the_first_token_fails_over_invisibly(error_class):
    """The client sees one clean stream and never learns the first provider failed."""
    a = ScriptedAdapter("a", [failed(error_class)])
    b = ScriptedAdapter("b", [*tokens(3), done()])
    rec = Recorder()
    rtr, _ = router({"a": a, "b": b}, observer=rec)

    events = await run(rtr)
    assert isinstance(events[-1], StreamDone)
    assert [e.text for e in events if isinstance(e, TokenChunk)] == ["t0 ", "t1 ", "t2 "]
    assert not any(isinstance(e, StreamFailed) for e in events)
    assert rec.failovers == [("a", "b", error_class.value)]
    assert b.calls == 1


async def test_failover_does_not_duplicate_delivered_output():
    """Exactly one response reaches the client, not a concatenation of two."""
    a = ScriptedAdapter("a", [failed(ErrorClass.SERVER_ERROR)])
    b = ScriptedAdapter("b", [*tokens(2), done(2)])
    rtr, _ = router({"a": a, "b": b})

    events = await run(rtr)
    assert len([e for e in events if isinstance(e, TokenChunk)]) == 2
    assert len([e for e in events if isinstance(e, StreamDone)]) == 1


async def test_a_caller_error_is_not_retried_elsewhere():
    """A malformed request fails identically everywhere; retrying multiplies the cost."""
    a = ScriptedAdapter("a", [failed(ErrorClass.BAD_REQUEST)])
    b = ScriptedAdapter("b", [*tokens(3), done()])
    rec = Recorder()
    rtr, _ = router({"a": a, "b": b}, observer=rec)

    events = await run(rtr)
    assert isinstance(events[-1], StreamFailed)
    assert events[-1].error_class is ErrorClass.BAD_REQUEST
    assert b.calls == 0
    assert rec.failovers == []


# -- no failover after delivery --------------------------------------------


async def test_a_failure_after_the_first_token_terminates_instead_of_failing_over():
    """Switching mid-response would hand the client two different answers spliced.

    A visible error can be retried by the caller; silently corrupted output
    cannot even be detected.
    """
    a = ScriptedAdapter("a", [*tokens(4), failed(ErrorClass.TIMEOUT_TOKEN, emitted=4)])
    b = ScriptedAdapter("b", [*tokens(3), done()])
    rec = Recorder()
    rtr, _ = router({"a": a, "b": b}, observer=rec)

    events = await run(rtr)
    terminal = events[-1]
    assert isinstance(terminal, StreamFailed)
    assert terminal.error_class is ErrorClass.TIMEOUT_TOKEN
    assert terminal.chunks_emitted == 4, "the client is told exactly what it received"
    assert b.calls == 0, "the second provider must not be consulted"
    assert rec.failovers == []
    assert rec.terminal == [("a", "timeout_token", True)]


async def test_delivered_tokens_are_kept_when_the_stream_then_fails():
    a = ScriptedAdapter("a", [*tokens(3), failed(ErrorClass.DISCONNECTED, emitted=3)])
    rtr, _ = router({"a": a}, route=("a",))

    events = await run(rtr)
    assert [e.text for e in events if isinstance(e, TokenChunk)] == ["t0 ", "t1 ", "t2 "]
    assert isinstance(events[-1], StreamFailed)


# -- exhaustion ------------------------------------------------------------


async def test_when_every_provider_fails_the_error_names_what_was_tried():
    a = ScriptedAdapter("a", [failed(ErrorClass.SERVER_ERROR)])
    b = ScriptedAdapter("b", [failed(ErrorClass.CONNECT)])
    rtr, _ = router({"a": a, "b": b})

    events = await run(rtr)
    terminal = events[-1]
    assert isinstance(terminal, StreamFailed)
    assert "all providers" in terminal.message
    assert "a: server_error" in terminal.message and "b: connect" in terminal.message
    assert terminal.chunks_emitted == 0


async def test_a_single_candidate_failure_is_terminal_even_before_the_first_token():
    a = ScriptedAdapter("a", [failed(ErrorClass.SERVER_ERROR)])
    rtr, _ = router({"a": a}, route=("a",))
    terminal = (await run(rtr))[-1]
    assert isinstance(terminal, StreamFailed)
    assert terminal.error_class is ErrorClass.SERVER_ERROR
    assert "a: server_error" in terminal.message


async def test_an_unconfigured_model_produces_a_clear_failure():
    rtr, _ = router({"a": ScriptedAdapter("a", [])}, route=())
    assert rtr.knows("chat") is False
    terminal = (await run(rtr))[-1]
    assert isinstance(terminal, StreamFailed)
    assert "no providers are configured" in terminal.message


# -- breaker interaction ---------------------------------------------------


async def test_an_open_breaker_is_skipped_and_the_next_provider_used():
    a = ScriptedAdapter("a", [failed(ErrorClass.SERVER_ERROR)])
    b = ScriptedAdapter("b", [*tokens(2), done(2)])
    rec = Recorder()
    rtr, registry = router({"a": a, "b": b}, policy=BreakerPolicy(min_samples=2), observer=rec)

    for _ in range(3):
        await run(rtr)

    assert registry.get("a").state is BreakerState.OPEN
    calls_before = a.calls
    events = await run(rtr)

    assert isinstance(events[-1], StreamDone)
    assert a.calls == calls_before, "an open breaker means the provider is not called at all"
    assert "a" in rec.skipped


async def test_all_breakers_open_gives_a_terminal_failure_without_calling_anyone():
    a = ScriptedAdapter("a", [failed(ErrorClass.SERVER_ERROR)])
    b = ScriptedAdapter("b", [failed(ErrorClass.SERVER_ERROR)])
    rtr, registry = router({"a": a, "b": b}, policy=BreakerPolicy(min_samples=2))

    for _ in range(3):
        await run(rtr)
    assert registry.get("a").state is BreakerState.OPEN
    assert registry.get("b").state is BreakerState.OPEN

    calls = (a.calls, b.calls)
    terminal = (await run(rtr))[-1]
    assert isinstance(terminal, StreamFailed)
    assert (a.calls, b.calls) == calls
    assert "skipped_breaker_open" in terminal.message


async def test_a_success_records_health_for_the_provider_that_served_it():
    a = ScriptedAdapter("a", [failed(ErrorClass.SERVER_ERROR)])
    b = ScriptedAdapter("b", [*tokens(2), done(2)])
    rtr, registry = router({"a": a, "b": b})
    await run(rtr)

    assert registry.get("a").snapshot()["failures"] == 1
    assert registry.get("b").snapshot()["successes"] == 1


# -- cancellation ----------------------------------------------------------


async def test_abandoning_a_stream_releases_the_probe_slot():
    """A leaked half-open probe would stop the breaker ever closing again."""
    a = ScriptedAdapter("a", [*tokens(10), done(10)])
    registry = HealthRegistry(("a",), BreakerPolicy(min_samples=2, half_open_probes=1))
    health = registry.get("a")
    health.record_failure(ErrorClass.SERVER_ERROR)
    health.record_failure(ErrorClass.SERVER_ERROR)
    assert health.state is BreakerState.OPEN

    health._reopen_at = 0.0                      # cooldown elapsed
    assert health.state is BreakerState.HALF_OPEN

    rtr = ProviderRouter({"a": a}, {"chat": ("a",)}, registry)
    stream = rtr.stream(REQUEST)
    assert isinstance(await anext(stream), TokenChunk)
    await stream.aclose()                        # client walks away mid-stream

    assert health.allow() is True, "the probe slot must be released, not leaked"

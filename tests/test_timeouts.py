"""Timeout decomposition.

Each deadline has to produce a *distinct* error class, because what happens next
depends on which one fired. A provider that never sent a first token can be
retried elsewhere invisibly; one that stalled halfway through cannot. A single
overall timeout collapses that distinction and makes both wait its full duration
before anything notices.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from switchyard.adapters.synthetic import SyntheticAdapter, build_client
from switchyard.core.config import TimeoutPolicy
from switchyard.synthetic.profiles import FaultSpec, ProviderProfile
from switchyard.types import CompletionRequest, ErrorClass, Message, StreamDone, StreamFailed

REQUEST = CompletionRequest(
    model="quick", messages=(Message(role="user", content="hi"),),
    max_tokens=4096, temperature=0.0, stream=True, request_id="t-1",
)


def profile(**over) -> ProviderProfile:
    base = {
        "name": "quick", "ttft_median_ms": 5.0, "ttft_sigma": 0.01,
        "output_tokens_median": 20.0, "output_tokens_sigma": 0.01,
        "tokens_per_second": 2000.0, "token_jitter_sigma": 0.0,
    }
    return ProviderProfile(**(base | over))


async def collect(fleet_server, timeouts: TimeoutPolicy, consume_delay: float = 0.0):
    """Run one request through the adapter, returning every event."""
    client = build_client(timeouts=timeouts)
    try:
        adapter = SyntheticAdapter("quick", fleet_server.base_url, client, timeouts)
        events = []
        async for event in adapter.stream(REQUEST):
            events.append(event)
            if consume_delay:
                await asyncio.sleep(consume_delay)
        return events
    finally:
        await client.aclose()


def terminal(events):
    assert events, "a stream must always produce a terminal event"
    return events[-1]


# -- each deadline produces its own error class ----------------------------


async def test_a_provider_that_never_sends_a_first_token_gives_timeout_ttft(fleet_server):
    """The recoverable case: nothing reached the client, so a retry is invisible."""
    fleet_server.state.profiles["quick"] = profile(ttft_median_ms=30_000.0)

    started = time.perf_counter()
    events = await collect(fleet_server, TimeoutPolicy(ttft_s=0.3, total_s=30.0))
    elapsed = time.perf_counter() - started

    failure = terminal(events)
    assert isinstance(failure, StreamFailed)
    assert failure.error_class is ErrorClass.TIMEOUT_TTFT
    assert failure.chunks_emitted == 0, "nothing was delivered, so failover stays transparent"
    assert elapsed < 3.0, "must fail on its own deadline, not the total one"


async def test_a_stream_that_stalls_midway_gives_timeout_token(fleet_server):
    """Distinct from TTFT: tokens already reached the client, so failover is not free."""
    fleet_server.state.profiles["quick"] = profile(
        faults=FaultSpec(stall_rate=1.0, stall_after_chunks=3, stall_seconds=30.0)
    )

    started = time.perf_counter()
    events = await collect(fleet_server, TimeoutPolicy(ttft_s=5.0, inter_token_s=0.3))
    elapsed = time.perf_counter() - started

    failure = terminal(events)
    assert isinstance(failure, StreamFailed)
    assert failure.error_class is ErrorClass.TIMEOUT_TOKEN
    assert failure.chunks_emitted == 3, "reports exactly what the client received"
    assert elapsed < 3.0


async def test_an_over_long_response_gives_timeout_total(fleet_server):
    """The backstop, when no individual gap is long enough to trip the others."""
    fleet_server.state.profiles["quick"] = profile(
        output_tokens_median=400.0, tokens_per_second=40.0
    )
    events = await collect(
        fleet_server, TimeoutPolicy(ttft_s=5.0, inter_token_s=5.0, total_s=0.5)
    )
    failure = terminal(events)
    assert isinstance(failure, StreamFailed)
    assert failure.error_class is ErrorClass.TIMEOUT_TOTAL
    assert failure.chunks_emitted > 0


async def test_an_unreachable_provider_gives_connect(fleet_server):
    timeouts = TimeoutPolicy(connect_s=0.5)
    client = build_client(timeouts=timeouts)
    try:
        # Port 1 is reserved and nothing listens on it.
        adapter = SyntheticAdapter("quick", "http://127.0.0.1:1", client, timeouts)
        events = [e async for e in adapter.stream(REQUEST)]
    finally:
        await client.aclose()

    failure = terminal(events)
    assert isinstance(failure, StreamFailed)
    assert failure.error_class is ErrorClass.CONNECT
    assert failure.chunks_emitted == 0


# -- the deadlines measure the provider, not the client --------------------


async def test_a_slow_consumer_is_not_mistaken_for_a_stalled_provider(fleet_server):
    """The per-chunk deadline wraps the await on the provider, never the yield.

    Otherwise backpressure from a slow client would be reported as a provider
    stall, and the breaker would open for something the provider did correctly.
    """
    fleet_server.state.profiles["quick"] = profile(output_tokens_median=8.0)

    # Consumer pauses far longer between chunks than the inter-token deadline.
    events = await collect(
        fleet_server, TimeoutPolicy(ttft_s=2.0, inter_token_s=0.1, total_s=30.0),
        consume_delay=0.25,
    )
    assert isinstance(terminal(events), StreamDone)


# -- health attribution ----------------------------------------------------


@pytest.mark.parametrize(
    ("error_class", "counts"),
    [
        (ErrorClass.CONNECT, True),
        (ErrorClass.TIMEOUT_TTFT, True),
        (ErrorClass.TIMEOUT_TOKEN, True),
        (ErrorClass.SERVER_ERROR, True),
        (ErrorClass.RATE_LIMITED, True),
        (ErrorClass.BAD_REQUEST, False),
        (ErrorClass.TIMEOUT_TOTAL, False),
    ],
)
def test_only_provider_faults_count_against_provider_health(error_class, counts):
    """A caller's bad request must not take a healthy provider away from everyone."""
    assert error_class.counts_against_provider is counts


def test_only_provider_faults_are_worth_retrying_elsewhere():
    assert ErrorClass.BAD_REQUEST.retryable is False
    assert ErrorClass.SERVER_ERROR.retryable is True


# -- a healthy stream is unaffected ----------------------------------------


async def test_generous_deadlines_do_not_interfere_with_a_normal_stream(fleet_server):
    fleet_server.state.profiles["quick"] = profile()
    events = await collect(fleet_server, TimeoutPolicy())
    assert isinstance(terminal(events), StreamDone)
    assert sum(1 for e in events if not isinstance(e, StreamDone)) > 0

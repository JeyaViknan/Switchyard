"""Circuit breaker and provider health.

Uses an injected clock so state transitions are exercised directly rather than
by sleeping, which keeps the tests fast and removes the timing flakiness that
would otherwise make a breaker test unreliable.
"""

from __future__ import annotations

import pytest

from switchyard.core.health import (
    BreakerPolicy,
    BreakerState,
    HealthRegistry,
    ProviderHealth,
)
from switchyard.types import ErrorClass


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def health(**over) -> tuple[ProviderHealth, Clock]:
    clock = Clock()
    policy = BreakerPolicy(**({"min_samples": 4, "window": 10, "cooldown_s": 5.0} | over))
    policy.validate()
    # Fixed jitter source so cooldowns are exact in tests.
    return ProviderHealth(name="p", policy=policy, clock=clock,
                          jitter_source=lambda: 0.5), clock


def fail(h: ProviderHealth, n: int, cls: ErrorClass = ErrorClass.SERVER_ERROR) -> None:
    for _ in range(n):
        h.record_failure(cls)


# -- tripping --------------------------------------------------------------


def test_a_healthy_provider_stays_closed():
    h, _ = health()
    for _ in range(20):
        h.record_success(ttft_s=0.2)
    assert h.state is BreakerState.CLOSED
    assert h.allow() is True


def test_a_single_failure_does_not_trip_the_breaker():
    """Without a minimum sample count, one unlucky request is a 100% failure rate."""
    h, _ = health()
    fail(h, 1)
    assert h.state is BreakerState.CLOSED


def test_a_sustained_failure_rate_opens_the_breaker():
    h, _ = health(min_samples=4, failure_threshold=0.5)
    fail(h, 4)
    assert h.state is BreakerState.OPEN
    assert h.allow() is False


def test_failures_below_the_threshold_do_not_trip():
    h, _ = health(min_samples=4, failure_threshold=0.6, window=10)
    for _ in range(6):
        h.record_success()
    fail(h, 3)                                    # 3/9 = 33%
    assert h.state is BreakerState.CLOSED


def test_a_caller_error_never_opens_the_breaker():
    """One tenant's malformed requests must not take the provider from everyone."""
    h, _ = health()
    fail(h, 50, ErrorClass.BAD_REQUEST)
    assert h.state is BreakerState.CLOSED
    assert h.snapshot()["errors"]["bad_request"] == 50


def test_a_total_timeout_does_not_open_the_breaker():
    """It is wall-clock including writing to the client, so a slow consumer causes it."""
    h, _ = health()
    fail(h, 50, ErrorClass.TIMEOUT_TOTAL)
    assert h.state is BreakerState.CLOSED


@pytest.mark.parametrize(
    "error_class",
    [ErrorClass.CONNECT, ErrorClass.TIMEOUT_TTFT, ErrorClass.TIMEOUT_TOKEN,
     ErrorClass.SERVER_ERROR, ErrorClass.RATE_LIMITED, ErrorClass.DISCONNECTED],
)
def test_provider_faults_open_the_breaker(error_class):
    h, _ = health(min_samples=4)
    fail(h, 4, error_class)
    assert h.state is BreakerState.OPEN


# -- recovery --------------------------------------------------------------


def test_the_breaker_half_opens_after_the_cooldown():
    h, clock = health(cooldown_s=5.0, jitter=0.0)
    fail(h, 4)
    assert h.allow() is False

    clock.advance(4.0)
    assert h.state is BreakerState.OPEN

    clock.advance(2.0)
    assert h.state is BreakerState.HALF_OPEN
    assert h.allow() is True


def test_half_open_admits_only_a_few_probes():
    """A provider that is still broken should reject a handful, not a flood."""
    h, clock = health(cooldown_s=1.0, jitter=0.0, half_open_probes=2)
    fail(h, 4)
    clock.advance(2.0)

    assert [h.allow() for _ in range(5)] == [True, True, False, False, False]


def test_a_successful_probe_closes_the_breaker():
    h, clock = health(cooldown_s=1.0, jitter=0.0)
    fail(h, 4)
    clock.advance(2.0)
    assert h.allow() is True

    h.record_success(ttft_s=0.1)
    assert h.state is BreakerState.CLOSED
    assert h.allow() is True


def test_a_failed_probe_reopens_the_breaker_with_a_longer_cooldown():
    """Repeated failure has to back off, or recovery becomes a retry loop."""
    h, clock = health(cooldown_s=5.0, jitter=0.0)
    fail(h, 4)
    first = h.snapshot()["reopens_in_s"]

    clock.advance(6.0)
    assert h.allow() is True
    h.record_failure(ErrorClass.SERVER_ERROR)

    assert h.state is BreakerState.OPEN
    assert h.snapshot()["reopens_in_s"] > first
    assert h.snapshot()["consecutive_trips"] == 2


def test_cooldown_is_capped():
    h, clock = health(cooldown_s=5.0, max_cooldown_s=12.0, jitter=0.0)
    for _ in range(8):
        fail(h, 4)
        clock.advance(1000.0)
        h.allow()
        h.record_failure(ErrorClass.SERVER_ERROR)
    assert h.snapshot()["reopens_in_s"] <= 12.0


def test_cooldown_is_jittered_so_recovery_is_not_synchronised():
    """Without jitter everything waiting on a provider retries at the same instant."""
    reopen_times = set()
    for source in (lambda: 0.0, lambda: 0.5, lambda: 1.0):
        clock = Clock()
        h = ProviderHealth(
            name="p", policy=BreakerPolicy(min_samples=4, cooldown_s=10.0, jitter=0.5),
            clock=clock, jitter_source=source,
        )
        fail(h, 4)
        reopen_times.add(h.snapshot()["reopens_in_s"])
    assert len(reopen_times) == 3, "identical cooldowns would synchronise retries"


def test_an_abandoned_probe_is_released():
    """A leaked probe slot would stop the breaker ever gathering enough evidence."""
    h, clock = health(cooldown_s=1.0, jitter=0.0, half_open_probes=1)
    fail(h, 4)
    clock.advance(2.0)

    assert h.allow() is True
    assert h.allow() is False, "the single probe slot is taken"

    h.record_abandoned()
    assert h.allow() is True, "releasing it lets the breaker try again"


# -- reporting -------------------------------------------------------------


def test_snapshot_reports_state_and_error_breakdown():
    h, _ = health()
    h.record_success(ttft_s=0.4)
    h.record_failure(ErrorClass.SERVER_ERROR)
    snapshot = h.snapshot()
    assert snapshot["state"] == "closed"
    assert snapshot["successes"] == 1 and snapshot["failures"] == 1
    assert snapshot["errors"] == {"server_error": 1}
    assert snapshot["ttft_ewma_ms"] == 400.0


def test_registry_tracks_each_provider_independently():
    registry = HealthRegistry(("a", "b"), BreakerPolicy(min_samples=4))
    for _ in range(4):
        registry.get("a").record_failure(ErrorClass.SERVER_ERROR)
    assert registry.get("a").state is BreakerState.OPEN
    assert registry.get("b").state is BreakerState.CLOSED
    assert set(registry.snapshot()) == {"a", "b"}


@pytest.mark.parametrize(
    "kwargs",
    [{"failure_threshold": 0}, {"failure_threshold": 1.5}, {"min_samples": 0},
     {"window": 2, "min_samples": 5}, {"cooldown_s": 0}, {"half_open_probes": 0}],
)
def test_invalid_breaker_policy_is_rejected(kwargs):
    with pytest.raises(ValueError):
        BreakerPolicy(**kwargs).validate()

"""Provider health tracking and circuit breaking.

Why a breaker at all
--------------------
When a provider is failing, continuing to send it traffic is worse than useless.
Every attempt costs a capacity slot for as long as the failure takes to detect,
and under load that means the gateway spends most of its concurrency discovering
the same outage over and over. A breaker turns a slow repeated discovery into
one fast local decision.

What counts as the provider's fault
-----------------------------------
Only failures the provider is actually responsible for. A malformed request
fails identically everywhere, so letting one tenant's bad requests open the
breaker would take a healthy provider away from every other tenant. A total
timeout is wall-clock across the whole response, including time spent writing to
a slow client, so it is not evidence about the provider either. `ErrorClass`
carries that distinction.

Recovery without a stampede
---------------------------
An open breaker closes by trying again, and the danger is that everything tries
at the same instant. Two things prevent that. Only a small number of probes are
admitted while half-open, so a provider that is still broken rejects a handful of
requests rather than a flood. And the cooldown is jittered and grows with each
successive trip, so repeated failures back off instead of hammering on a fixed
cycle.
"""

from __future__ import annotations

import enum
import random
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from switchyard.types import ErrorClass


class BreakerState(enum.StrEnum):
    CLOSED = "closed"        # traffic flows
    OPEN = "open"            # provider is failing; do not send
    HALF_OPEN = "half_open"  # let a few through to see if it recovered


@dataclass(frozen=True, slots=True)
class BreakerPolicy:
    """Tuning for the breaker.

    `min_samples` matters more than it looks: without it, the first failure
    after a quiet period is a 100% failure rate and would open the breaker on a
    single unlucky request.
    """

    failure_threshold: float = 0.5
    min_samples: int = 10
    window: int = 50
    cooldown_s: float = 5.0
    max_cooldown_s: float = 60.0
    jitter: float = 0.3
    half_open_probes: int = 2

    def validate(self) -> None:
        if not 0 < self.failure_threshold <= 1:
            raise ValueError("failure_threshold must be in (0, 1]")
        if self.min_samples < 1 or self.window < self.min_samples:
            raise ValueError("window must be >= min_samples >= 1")
        if self.cooldown_s <= 0 or self.max_cooldown_s < self.cooldown_s:
            raise ValueError("max_cooldown_s must be >= cooldown_s > 0")
        if self.half_open_probes < 1:
            raise ValueError("half_open_probes must be >= 1")


@dataclass(slots=True)
class ProviderHealth:
    """Breaker and rolling statistics for one provider."""

    name: str
    policy: BreakerPolicy = BreakerPolicy()
    clock: Callable[[], float] = time.monotonic
    jitter_source: Callable[[], float] = random.random

    _state: BreakerState = BreakerState.CLOSED
    _outcomes: deque[bool] = field(default_factory=deque)
    _reopen_at: float = 0.0
    _consecutive_trips: int = 0
    _probes_in_flight: int = 0
    _ttft_ewma_s: float | None = None
    _errors: dict[str, int] = field(default_factory=dict)
    _successes: int = 0
    _failures: int = 0

    @property
    def state(self) -> BreakerState:
        """Current state, advancing OPEN to HALF_OPEN once the cooldown has passed."""
        if self._state is BreakerState.OPEN and self.clock() >= self._reopen_at:
            self._state = BreakerState.HALF_OPEN
            self._probes_in_flight = 0
        return self._state

    def allow(self) -> bool:
        """Whether to send a request now. Reserves a probe slot when half-open."""
        state = self.state
        if state is BreakerState.CLOSED:
            return True
        if state is BreakerState.OPEN:
            return False
        if self._probes_in_flight >= self.policy.half_open_probes:
            return False
        self._probes_in_flight += 1
        return True

    def record_success(self, ttft_s: float | None = None) -> None:
        self._successes += 1
        if ttft_s is not None:
            self._ttft_ewma_s = (
                ttft_s if self._ttft_ewma_s is None
                else 0.2 * ttft_s + 0.8 * self._ttft_ewma_s
            )
        if self._state is BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._close()
            return
        self._push(True)

    def record_failure(self, error_class: ErrorClass) -> None:
        self._failures += 1
        self._errors[error_class.value] = self._errors.get(error_class.value, 0) + 1

        if not error_class.counts_against_provider:
            # Still released if it was a probe, but not held against the provider.
            if self._state is BreakerState.HALF_OPEN:
                self._probes_in_flight = max(0, self._probes_in_flight - 1)
            return

        if self._state is BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)
            self._open()
            return

        self._push(False)
        if self._should_trip():
            self._open()

    def record_abandoned(self) -> None:
        """A probe was admitted but produced no verdict, e.g. the client left.

        Releasing it matters: a leaked probe slot means the breaker can never
        gather enough evidence to close, and the provider stays shut out.
        """
        if self._state is BreakerState.HALF_OPEN:
            self._probes_in_flight = max(0, self._probes_in_flight - 1)

    def _push(self, ok: bool) -> None:
        self._outcomes.append(ok)
        while len(self._outcomes) > self.policy.window:
            self._outcomes.popleft()

    def _should_trip(self) -> bool:
        if len(self._outcomes) < self.policy.min_samples:
            return False
        failures = sum(1 for ok in self._outcomes if not ok)
        return failures / len(self._outcomes) >= self.policy.failure_threshold

    def _open(self) -> None:
        self._state = BreakerState.OPEN
        self._consecutive_trips += 1
        # Exponential backoff on repeated trips, jittered so that everything
        # waiting on this provider does not retry at the same instant.
        base = min(
            self.policy.cooldown_s * (2 ** (self._consecutive_trips - 1)),
            self.policy.max_cooldown_s,
        )
        spread = base * self.policy.jitter
        self._reopen_at = self.clock() + base + (self.jitter_source() * 2 - 1) * spread
        self._outcomes.clear()

    def _close(self) -> None:
        self._state = BreakerState.CLOSED
        self._consecutive_trips = 0
        self._outcomes.clear()

    def snapshot(self) -> dict[str, object]:
        return {
            "state": self.state.value,
            "successes": self._successes,
            "failures": self._failures,
            "errors": dict(self._errors),
            "ttft_ewma_ms": (
                round(self._ttft_ewma_s * 1000, 1) if self._ttft_ewma_s is not None else None
            ),
            "reopens_in_s": (
                round(max(0.0, self._reopen_at - self.clock()), 2)
                if self._state is BreakerState.OPEN else None
            ),
            "consecutive_trips": self._consecutive_trips,
        }


class HealthRegistry:
    """Health for every configured provider."""

    def __init__(
        self, providers: tuple[str, ...], policy: object | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if policy is not None and not isinstance(policy, BreakerPolicy):
            # Accept the config-shaped equivalent so configuration does not have
            # to import the runtime policy type.
            policy = BreakerPolicy(**{
                f: getattr(policy, f) for f in BreakerPolicy.__slots__
            })
        policy = policy or BreakerPolicy()
        policy.validate()
        self._providers = {
            name: ProviderHealth(name=name, policy=policy, clock=clock) for name in providers
        }

    def get(self, name: str) -> ProviderHealth:
        health = self._providers.get(name)
        if health is None:
            health = self._providers[name] = ProviderHealth(name=name)
        return health

    def snapshot(self) -> dict[str, dict[str, object]]:
        return {name: h.snapshot() for name, h in self._providers.items()}

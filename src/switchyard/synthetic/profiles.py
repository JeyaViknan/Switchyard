"""Behavioral profiles for the synthetic provider fleet.

Why this exists
---------------
Every scheduling experiment compares policies against each other. That
comparison is only valid if the provider behaves *identically* under both
policies for the same request -- otherwise the measurement is provider noise,
not policy effect. Real APIs cannot give that guarantee, and they cost money and
rate-limit exactly when throughput is needed.

So the fleet is deterministic: every per-request draw (time to first token,
output length, whether this request fails) comes from an RNG seeded by
`(run_seed, request_id)`. Replaying the same workload under a different
scheduler reproduces the same provider behavior token for token.

Distributions
-------------
Time-to-first-token and output length are both right-skewed in practice, so both
are lognormal, parameterized by median and sigma rather than the underlying
normal's mu -- median is the number a person can reason about.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace

import numpy as np

from switchyard.types import ErrorClass


def request_rng(run_seed: int, request_id: str) -> np.random.Generator:
    """Derive a per-request RNG.

    Hashing rather than mixing arithmetically so that adjacent request ids do not
    produce correlated streams.
    """
    digest = hashlib.blake2b(f"{run_seed}:{request_id}".encode(), digest_size=8).digest()
    return np.random.default_rng(int.from_bytes(digest, "big"))


@dataclass(frozen=True, slots=True)
class FaultSpec:
    """Injected failure behavior. All probabilities are per request.

    These are separate knobs rather than one "failure mode" enum because the
    failure matrix distinguishes them by *where* they occur, which is what
    determines whether recovery can be transparent:

    - `error_rate` fails before any token is emitted   -> recoverable invisibly
    - `abort_rate` fails after `abort_after_chunks`    -> not recoverable
    - `stall_rate` stops sending without closing       -> only an inter-token
      timeout catches this; a total timeout would sit for its full duration
    """

    error_rate: float = 0.0
    error_class: ErrorClass = ErrorClass.SERVER_ERROR

    abort_rate: float = 0.0
    abort_after_chunks: int = 20

    stall_rate: float = 0.0
    stall_after_chunks: int = 10
    stall_seconds: float = 300.0

    def validate(self) -> None:
        for field_name in ("error_rate", "abort_rate", "stall_rate"):
            value = getattr(self, field_name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{field_name} must be in [0, 1], got {value}")


@dataclass(frozen=True, slots=True)
class ProviderProfile:
    """How one synthetic provider behaves.

    `tokens_per_second` drives inter-token delay. Real providers deliver tokens
    with jitter, not on a metronome, so a per-chunk multiplicative jitter is
    applied; without it the inter-token latency histogram is a single spike and
    tells you nothing about whether the pump is adding delay.
    """

    name: str

    ttft_median_ms: float = 300.0
    ttft_sigma: float = 0.4

    output_tokens_median: float = 180.0
    output_tokens_sigma: float = 0.7

    tokens_per_second: float = 60.0
    token_jitter_sigma: float = 0.25

    prompt_tokens_per_char: float = 0.25

    faults: FaultSpec = FaultSpec()

    def validate(self) -> None:
        if self.ttft_median_ms < 0:
            raise ValueError("ttft_median_ms must be >= 0")
        if self.tokens_per_second <= 0:
            raise ValueError("tokens_per_second must be > 0")
        if self.output_tokens_median <= 0:
            raise ValueError("output_tokens_median must be > 0")
        self.faults.validate()

    # -- per-request draws -------------------------------------------------

    def draw_ttft_seconds(self, rng: np.random.Generator) -> float:
        return float(rng.lognormal(np.log(self.ttft_median_ms), self.ttft_sigma)) / 1000.0

    def draw_output_tokens(self, rng: np.random.Generator, max_tokens: int) -> int:
        """Draw an output length, clipped to [1, max_tokens].

        The clip at `max_tokens` is what makes the capacity-accounting experiment
        meaningful: it reproduces the real situation where a caller declares a
        ceiling far above what the model actually emits, so reserving the ceiling
        wastes most of the reservation.
        """
        drawn = rng.lognormal(np.log(self.output_tokens_median), self.output_tokens_sigma)
        return int(np.clip(round(drawn), 1, max_tokens))

    def draw_inter_token_seconds(self, rng: np.random.Generator) -> float:
        base = 1.0 / self.tokens_per_second
        if self.token_jitter_sigma <= 0:
            return base
        return float(base * rng.lognormal(0.0, self.token_jitter_sigma))

    def with_faults(self, faults: FaultSpec) -> ProviderProfile:
        return replace(self, faults=faults)


# Default fleet. Two providers with deliberately different latency so that
# routing experiments have something to distinguish, plus a slow one so the
# heterogeneous-provider case is available without editing code.
DEFAULT_FLEET: dict[str, ProviderProfile] = {
    "fast": ProviderProfile(
        name="fast",
        ttft_median_ms=220.0,
        tokens_per_second=90.0,
        output_tokens_median=160.0,
    ),
    "slow": ProviderProfile(
        name="slow",
        ttft_median_ms=650.0,
        ttft_sigma=0.6,
        tokens_per_second=35.0,
        output_tokens_median=220.0,
    ),
}


@dataclass(frozen=True, slots=True)
class RequestPlan:
    """Every random decision for one request, drawn once.

    Drawing all decisions up front, in a fixed order, from a single seeded
    stream is what makes the fleet reproducible. The alternative -- deciding
    things lazily as the stream progresses -- makes the draw sequence depend on
    timing and on which faults happen to be configured, which silently breaks
    comparability between runs.
    """

    fail_before_first_token: bool
    error_class: ErrorClass
    n_tokens: int
    ttft_s: float
    abort_at: int | None
    stall_at: int | None
    stall_s: float
    inter_token_s: tuple[float, ...]


def plan_request(profile: ProviderProfile, run_seed: int, request_id: str,
                 max_tokens: int) -> RequestPlan:
    """Draw the full behavior of one request.

    Draw order is fixed and must not be reordered: doing so changes every
    previously recorded run's behavior for the same seed.
    """
    rng = request_rng(run_seed, request_id)
    faults = profile.faults

    fail_first = float(rng.random()) < faults.error_rate
    n_tokens = profile.draw_output_tokens(rng, max_tokens)
    ttft = profile.draw_ttft_seconds(rng)
    will_abort = float(rng.random()) < faults.abort_rate
    will_stall = float(rng.random()) < faults.stall_rate
    delays = tuple(profile.draw_inter_token_seconds(rng) for _ in range(max(0, n_tokens - 1)))

    return RequestPlan(
        fail_before_first_token=fail_first,
        error_class=faults.error_class,
        n_tokens=n_tokens,
        ttft_s=ttft,
        abort_at=faults.abort_after_chunks if will_abort else None,
        stall_at=faults.stall_after_chunks if will_stall else None,
        stall_s=faults.stall_seconds,
        inter_token_s=delays,
    )

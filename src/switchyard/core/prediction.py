"""Output-length prediction.

The scheduler needs to know what a request will cost before running it, and that
is exactly what nobody can tell it: output length is not declared by the caller
and not promised by the model. `max_tokens` is a ceiling, usually a wildly
pessimistic one -- a request declaring 4096 that emits 180 over-states its cost
by more than twenty times. Scheduling on the ceiling would idle most of the
gateway's capacity; scheduling on nothing would give up on fairness entirely.

So Switchyard predicts, and then corrects. Predictions are per (tenant, model),
because output length is far more a property of what a tenant asks for than of
the model answering.

Why log space
-------------
Output lengths are right-skewed -- a long tail of much longer answers -- and an
arithmetic mean sits well above the typical value while an arithmetic standard
deviation implies negative lengths at the low end. Tracking the mean and
variance of `log(tokens)` fits the shape, keeps every derived quantile positive,
and costs the same two floats.

Two quantiles, two uses
-----------------------
`p50` is the scheduler's cost estimate: an unbiased guess at what the request
will consume, which fairness accounting later corrects against the real figure.
`p95` is the budget reservation: deliberately pessimistic, because the cost of
over-reserving is some idle capacity while the cost of under-reserving is
overspending a tenant's budget.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

# Weight given to each new observation. 0.1 tracks a genuine shift in a tenant's
# workload within a few dozen requests without letting one outlier move the
# estimate far.
DEFAULT_ALPHA = 0.1

# Assumed median output before anything has been observed. Deliberately modest:
# under-estimating a new tenant briefly costs fairness accuracy, over-estimating
# it withholds capacity it might legitimately want.
COLD_START_TOKENS = 256.0

# Until this many observations exist, blend the estimate toward the cold-start
# prior so a single early request cannot define a tenant's profile.
WARMUP_SAMPLES = 5

Z_P95 = 1.6449


@dataclass(frozen=True, slots=True)
class Estimate:
    p50: float
    p95: float
    samples: int

    @property
    def confident(self) -> bool:
        return self.samples >= WARMUP_SAMPLES


@dataclass(slots=True)
class _Profile:
    """Running mean and variance of log(output tokens)."""

    mu: float = math.log(COLD_START_TOKENS)
    var: float = 0.5 ** 2
    samples: int = 0

    def observe(self, tokens: int, alpha: float) -> None:
        if tokens < 1:
            return
        x = math.log(tokens)
        if self.samples == 0:
            self.mu = x
        else:
            delta = x - self.mu
            self.mu += alpha * delta
            # EWMA of squared deviation, measured against the pre-update mean so
            # a single large jump registers as variance rather than being
            # absorbed silently into the mean.
            self.var = (1 - alpha) * (self.var + alpha * delta * delta)
        self.samples += 1


class OutputLengthPredictor:
    """Per (tenant, model) estimates, corrected by observation."""

    def __init__(self, alpha: float = DEFAULT_ALPHA) -> None:
        self._alpha = alpha
        self._profiles: dict[tuple[str, str], _Profile] = {}

    def estimate(self, tenant_id: str, model: str, max_tokens: int) -> Estimate:
        profile = self._profiles.get((tenant_id, model))
        if profile is None or profile.samples == 0:
            return Estimate(
                p50=min(COLD_START_TOKENS, max_tokens),
                p95=float(max_tokens),      # no information: assume the worst
                samples=0,
            )

        sigma = math.sqrt(max(profile.var, 1e-9))
        p50 = math.exp(profile.mu)
        p95 = math.exp(profile.mu + Z_P95 * sigma)

        if profile.samples < WARMUP_SAMPLES:
            # Blend toward the prior while the sample is thin.
            blend = profile.samples / WARMUP_SAMPLES
            p50 = blend * p50 + (1 - blend) * COLD_START_TOKENS
            p95 = blend * p95 + (1 - blend) * max_tokens

        return Estimate(
            p50=float(min(max(p50, 1.0), max_tokens)),
            p95=float(min(max(p95, p50), max_tokens)),
            samples=profile.samples,
        )

    def observe(self, tenant_id: str, model: str, tokens: int) -> None:
        key = (tenant_id, model)
        profile = self._profiles.get(key)
        if profile is None:
            profile = self._profiles[key] = _Profile()
        profile.observe(tokens, self._alpha)

    def snapshot(self) -> dict[tuple[str, str], Estimate]:
        """Current estimates, for diagnostics."""
        return {
            key: self.estimate(key[0], key[1], 1_000_000)
            for key in self._profiles
        }

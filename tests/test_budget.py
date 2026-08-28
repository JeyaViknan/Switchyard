"""Token budget accounting.

The invariant under test is that a tenant's settled spend never exceeds its
limit. That is not a statistical property here -- it holds by construction,
because a request's reservation is its ceiling rather than its predicted length,
and because the ceiling is what the provider is actually told.

The three quantities the accounting must not confuse:
  estimated work    -- p50, used only to order the queue
  reserved budget   -- the request's effective max_tokens, held while in flight
  actual consumption -- what the provider emitted, charged on completion
"""

from __future__ import annotations

import pytest

from switchyard.core.budget import (
    MIN_USEFUL_TOKENS,
    BudgetExceeded,
    BudgetLedger,
    TenantBudget,
)
from switchyard.core.config import Tenant


def ledger(limit: int | None = 1000, tenant_id: str = "t1") -> BudgetLedger:
    return BudgetLedger({tenant_id: TenantBudget(limit=limit)})


def from_tenants(**budgets: int | None) -> BudgetLedger:
    return BudgetLedger.from_tenants(tuple(
        Tenant(id=tid, key_sha256="a" * 64, budget_tokens=limit)
        for tid, limit in budgets.items()
    ))


# -- reserve and settle ----------------------------------------------------


def test_reservation_is_the_ceiling_not_the_estimate():
    """The distinction the whole design rests on.

    A predicted length would be right most of the time, and 'most of the time'
    is not a spending bound.
    """
    led = ledger(1000)
    with led.reserve("t1", requested_max_tokens=400) as reservation:
        assert reservation.reserved == 400
        assert led.remaining("t1") == 600, "the full ceiling is held while in flight"
        reservation.actual = 30
    assert led.remaining("t1") == 970, "only what was used is charged"


def test_unused_reservation_is_returned_on_completion():
    led = ledger(1000)
    with led.reserve("t1", 500) as r:
        r.actual = 120
    snapshot = led.snapshot()["t1"]
    assert snapshot["spent"] == 120
    assert snapshot["reserved"] == 0


def test_a_request_that_produced_nothing_costs_nothing():
    """Cancellation and pre-first-token failure both land here."""
    led = ledger(1000)
    with led.reserve("t1", 500):
        pass                                  # actual never set
    assert led.remaining("t1") == 1000
    assert led.snapshot()["t1"]["spent"] == 0


def test_a_partially_completed_request_is_charged_for_what_it_emitted():
    """A stream that died after 40 tokens cost 40 tokens; the provider billed them."""
    led = ledger(1000)
    with led.reserve("t1", 500) as r:
        r.actual = 40
    assert led.snapshot()["t1"]["spent"] == 40


def test_reservation_is_released_even_when_the_body_raises():
    led = ledger(1000)
    with pytest.raises(RuntimeError), led.reserve("t1", 500) as r:
        r.actual = 10
        raise RuntimeError("provider exploded")
    assert led.remaining("t1") == 990
    assert led.snapshot()["t1"]["reserved"] == 0


def test_concurrent_reservations_each_hold_their_own_ceiling():
    led = ledger(1000)
    with led.reserve("t1", 300) as a, led.reserve("t1", 300) as b:
        assert led.remaining("t1") == 400
        a.actual, b.actual = 50, 60
    assert led.snapshot()["t1"]["spent"] == 110
    assert led.remaining("t1") == 890


# -- clamping near the limit -----------------------------------------------


def test_max_tokens_is_clamped_to_remaining_headroom():
    """Better to truncate near exhaustion than to overspend or refuse outright."""
    led = ledger(200)
    with led.reserve("t1", requested_max_tokens=4096) as r:
        assert r.clamped is True
        assert r.effective_max_tokens == 200
        assert r.requested_max_tokens == 4096
        assert led.remaining("t1") == 0


def test_no_clamp_when_headroom_is_ample():
    led = ledger(10_000)
    with led.reserve("t1", 512) as r:
        assert r.clamped is False
        assert r.effective_max_tokens == 512


def test_exhausted_budget_is_refused_rather_than_served_as_a_stub():
    led = ledger(MIN_USEFUL_TOKENS - 1)
    assert led.has_headroom("t1") is False
    with pytest.raises(BudgetExceeded) as exc, led.reserve("t1", 512):
        pass
    assert exc.value.remaining == MIN_USEFUL_TOKENS - 1


def test_headroom_check_accounts_for_in_flight_reservations():
    """Otherwise a burst all passes the check and then overspends together."""
    led = ledger(600)
    with led.reserve("t1", 590):
        assert led.has_headroom("t1") is False


# -- the hard bound --------------------------------------------------------


def test_spend_never_exceeds_the_limit_however_requests_interleave():
    """The invariant, exercised against adversarial actuals.

    Every request tries to emit far more than typical. Because the reservation
    is the ceiling and the ceiling is what the provider is told, actual can
    never exceed what was held.
    """
    limit = 1000
    led = ledger(limit)
    served = 0
    for _ in range(200):
        if not led.has_headroom("t1"):
            break
        with led.reserve("t1", 4096) as r:
            # A provider that always runs to the clamp: the worst case.
            r.actual = r.effective_max_tokens
            served += 1
    spent = led.snapshot()["t1"]["spent"]
    assert spent <= limit, f"overspent: {spent} > {limit}"
    assert served > 0


def test_actual_above_the_reservation_cannot_overspend():
    """Defensive: if a provider ever ignored max_tokens, the charge is still capped.

    This should be unreachable -- the clamp is passed to the provider -- but the
    ledger is the last place that could turn a provider bug into an overspend.
    """
    led = ledger(500)
    with led.reserve("t1", 500) as r:
        r.actual = 99_999
    assert led.snapshot()["t1"]["spent"] == 500
    assert led.remaining("t1") == 0


# -- unlimited -------------------------------------------------------------


def test_a_tenant_without_a_limit_is_never_clamped_or_refused():
    led = from_tenants(free=None)
    assert led.has_headroom("free") is True
    with led.reserve("free", 4096) as r:
        assert r.clamped is False
        assert r.effective_max_tokens == 4096
        r.actual = 4096
    assert led.snapshot()["free"]["limit"] is None
    assert led.snapshot()["free"]["available"] is None


def test_budgets_are_per_tenant():
    led = from_tenants(a=100, b=100)
    with led.reserve("a", 100) as r:
        r.actual = 100
    assert led.remaining("a") == 0
    assert led.remaining("b") == 100
    assert led.has_headroom("b") is True

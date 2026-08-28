"""Queue ordering and fairness.

These are pure and synchronous: ordering is a property of the policy, not of
timing, so testing it through the async scheduler would add flakiness without
adding coverage.
"""

from __future__ import annotations

import pytest

from switchyard.core.queueing import FifoQueue, Waiter, WeightedFairQueue, build_policy

ALWAYS = lambda _tenant: True  # noqa: E731


def waiter(tenant: str, cost: float = 100.0, deadline_at: float = 1e9) -> Waiter:
    return Waiter(tenant_id=tenant, cost=cost, deadline_at=deadline_at, enqueued_at=0.0)


def drain(policy, eligible=ALWAYS, limit: int = 1000) -> list[Waiter]:
    out = []
    while len(out) < limit:
        nxt = policy.take_next(eligible)
        if nxt is None:
            return out
        out.append(nxt)
    return out


# -- FIFO ------------------------------------------------------------------


def test_fifo_preserves_global_arrival_order():
    q = FifoQueue()
    order = ["a", "b", "a", "c", "b"]
    for tenant in order:
        q.enqueue(waiter(tenant))
    assert [w.tenant_id for w in drain(q)] == order


def test_fifo_lets_one_tenant_dominate_the_queue():
    """The baseline's failure mode, demonstrated rather than asserted.

    A tenant offering load faster than the others pushes their requests
    arbitrarily far back, because arrival order is the only thing FIFO knows.
    """
    q = FifoQueue()
    for _ in range(20):
        q.enqueue(waiter("noisy"))
    q.enqueue(waiter("quiet"))

    served = drain(q)
    position = [w.tenant_id for w in served].index("quiet")
    assert position == 20, "the quiet tenant waits behind every noisy request"


# -- weighted fair queueing ------------------------------------------------


def test_equal_weights_produce_round_robin():
    q = WeightedFairQueue({"a": 1.0, "b": 1.0})
    for _ in range(4):
        q.enqueue(waiter("a"))
        q.enqueue(waiter("b"))
    assert [w.tenant_id for w in drain(q)] == ["a", "b"] * 4


def test_a_backlogged_tenant_cannot_monopolise_the_queue():
    """The same scenario as the FIFO test, with the fair policy."""
    q = WeightedFairQueue({"noisy": 1.0, "quiet": 1.0})
    for _ in range(20):
        q.enqueue(waiter("noisy"))
    q.enqueue(waiter("quiet"))

    served = drain(q)
    position = [w.tenant_id for w in served].index("quiet")
    assert position <= 1, f"quiet tenant served at position {position}, expected near-immediate"


def test_capacity_share_follows_weight():
    q = WeightedFairQueue({"big": 3.0, "small": 1.0})
    for _ in range(120):
        q.enqueue(waiter("big", cost=100))
        q.enqueue(waiter("small", cost=100))

    served = drain(q, limit=120)
    tokens = {"big": 0.0, "small": 0.0}
    for w in served:
        tokens[w.tenant_id] += w.cost

    ratio = tokens["big"] / tokens["small"]
    assert 2.7 < ratio < 3.3, f"weight 3:1 should give ~3x the tokens, got {ratio:.2f}"


def test_fairness_is_measured_in_tokens_not_requests():
    """The substance of the policy.

    Two tenants at equal weight, one issuing requests ten times larger. Equal
    treatment means equal *tokens*, so the tenant with larger requests must
    receive proportionally fewer of them. Counting requests would call a 10x
    capacity imbalance fair.
    """
    q = WeightedFairQueue({"heavy": 1.0, "light": 1.0})
    for _ in range(60):
        q.enqueue(waiter("heavy", cost=1000))
        q.enqueue(waiter("light", cost=100))

    served = drain(q, limit=60)
    counts = {"heavy": 0, "light": 0}
    tokens = {"heavy": 0.0, "light": 0.0}
    for w in served:
        counts[w.tenant_id] += 1
        tokens[w.tenant_id] += w.cost

    assert counts["light"] > counts["heavy"] * 5, "light tenant should get many more requests"
    assert 0.8 < tokens["heavy"] / tokens["light"] < 1.25, "token share should be roughly equal"


def test_an_idle_tenant_does_not_bank_credit_while_away():
    """Otherwise a tenant returning after a quiet period drains the gateway.

    Its virtual clock would be far behind everyone else's, so it would win every
    scheduling decision until the others caught up.
    """
    q = WeightedFairQueue({"steady": 1.0, "returning": 1.0})
    for _ in range(30):
        q.enqueue(waiter("steady"))
    drain(q, limit=15)                        # steady runs alone for a while

    q.enqueue(waiter("returning"))
    for _ in range(5):
        q.enqueue(waiter("returning"))

    served = [w.tenant_id for w in drain(q, limit=10)]
    assert served.count("returning") <= 6
    assert "steady" in served, "the returning tenant must not lock out the incumbent"


def test_ineligible_tenants_are_skipped_not_blocking():
    """A tenant at its own ceiling must not stall everyone behind it."""
    q = WeightedFairQueue({"blocked": 1.0, "free": 1.0})
    q.enqueue(waiter("blocked"))
    q.enqueue(waiter("free"))

    nxt = q.take_next(lambda t: t != "blocked")
    assert nxt is not None and nxt.tenant_id == "free"


def test_cancelled_waiters_are_skipped():
    q = WeightedFairQueue({"a": 1.0})
    first, second = waiter("a"), waiter("a")
    q.enqueue(first)
    q.enqueue(second)
    q.remove(first)

    assert q.depth("a") == 1
    assert q.take_next(ALWAYS) is second


def test_expired_waiters_are_returned_and_dequeued():
    q = WeightedFairQueue({"a": 1.0})
    stale = waiter("a", deadline_at=5.0)
    fresh = waiter("a", deadline_at=100.0)
    q.enqueue(stale)
    q.enqueue(fresh)

    assert q.expired(now=10.0) == [stale]
    assert q.depth("a") == 1
    assert q.take_next(ALWAYS) is fresh


def test_empty_queue_returns_nothing():
    assert WeightedFairQueue({}).take_next(ALWAYS) is None
    assert FifoQueue().take_next(ALWAYS) is None


def test_build_policy_rejects_unknown_names():
    assert isinstance(build_policy("fifo", {}), FifoQueue)
    assert isinstance(build_policy("drr", {}), WeightedFairQueue)
    with pytest.raises(ValueError, match="unknown scheduling policy"):
        build_policy("magic", {})


# -- settling estimates against reality ------------------------------------


def test_underestimated_cost_is_corrected_on_settle():
    """A tenant whose requests run longer than predicted must not gain share.

    Without settling, a systematic under-estimate would under-charge every
    request and compound into a permanently larger share of the gateway.
    """
    q = WeightedFairQueue({"a": 1.0, "b": 1.0})
    q.enqueue(waiter("a", cost=100))
    q.enqueue(waiter("b", cost=100))
    q.take_next(ALWAYS)
    q.take_next(ALWAYS)

    before = q.virtual_times()
    q.settle("a", estimated=100.0, actual=900.0)     # 'a' really used 9x
    after = q.virtual_times()

    assert after["a"] - before["a"] == pytest.approx(800.0)
    assert after["b"] == before["b"]


def test_settling_shifts_subsequent_scheduling_toward_the_other_tenant():
    q = WeightedFairQueue({"a": 1.0, "b": 1.0})
    for _ in range(6):
        q.enqueue(waiter("a", cost=100))
        q.enqueue(waiter("b", cost=100))

    q.take_next(ALWAYS)
    q.settle("a", estimated=100.0, actual=1000.0)

    served = [w.tenant_id for w in drain(q, limit=6)]
    assert served.count("b") > served.count("a"), (
        "after over-consuming, 'a' should be served less until the books balance"
    )


def test_settle_is_weighted():
    q = WeightedFairQueue({"heavy": 4.0})
    q.enqueue(waiter("heavy", cost=100))
    q.take_next(ALWAYS)
    before = q.virtual_times()["heavy"]
    q.settle("heavy", estimated=100.0, actual=500.0)
    assert q.virtual_times()["heavy"] - before == pytest.approx(100.0)   # 400 / 4


def test_settle_on_fifo_is_a_harmless_noop():
    q = FifoQueue()
    q.enqueue(waiter("a"))
    q.settle("a", estimated=10.0, actual=1000.0)
    assert q.depth("a") == 1

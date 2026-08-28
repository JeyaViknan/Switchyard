"""Token budgets: reservation, settlement, and a hard spending bound.

Three quantities, deliberately kept distinct
--------------------------------------------
Confusing these is the easiest way to get budget accounting wrong, so each has
one job and one source:

1. *Estimated work* -- the scheduler's p50 guess at output length. Used only to
   order the queue. Being wrong costs some short-term unfairness, and the error
   is corrected when the lease settles.
2. *Reserved budget* -- tokens held against a tenant's balance while a request
   is in flight. This is **not** the estimate. It is the request's effective
   `max_tokens`: the most the request could possibly consume.
3. *Actual consumption* -- what the provider really emitted, charged on
   completion, with the unused part of the reservation returned.

Why the reservation is the ceiling and not the estimate
-------------------------------------------------------
It is tempting to reserve the p95 estimate, since it is right most of the time
and wastes far less headroom. But "right most of the time" is a different
guarantee for budgets than for scheduling. A scheduling mis-estimate is
self-correcting: settlement adjusts the tenant's virtual clock and fairness
converges anyway. A budget mis-estimate is money already spent -- there is no
correcting it after the provider has generated the tokens.

So the reservation covers the worst case. Since a request can never emit more
than its `max_tokens`, reserving exactly that makes
`spent + sum(reserved) <= budget` an invariant that holds by construction, not
by probability.

The cost of that choice, and the clamp
--------------------------------------
Reserving the ceiling means a tenant with 500 tokens of headroom cannot start a
request declaring `max_tokens=4096`, even though it would probably have used
150. Rejecting it would be needlessly strict. Instead the request's `max_tokens`
is *clamped* to the remaining headroom: the response may be cut short, and the
client is told so, but the tenant spends exactly what it has and never a token
more. Truncating near exhaustion is a better failure than silently overspending.

Budgets here are lifetime totals. Refilling windows are a billing-period concern
and would need a clock and a reset policy; neither changes the accounting model
below.
"""

from __future__ import annotations

import enum
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from switchyard.core.config import Tenant

# Serving a request with only a handful of tokens of headroom produces a stub
# that costs a provider call and helps nobody. Below this, refuse instead.
MIN_USEFUL_TOKENS = 16


class BudgetOutcome(enum.StrEnum):
    OK = "ok"
    CLAMPED = "clamped"
    EXHAUSTED = "exhausted"


class BudgetExceeded(Exception):
    """The tenant has no usable headroom left."""

    def __init__(self, tenant_id: str, remaining: int) -> None:
        super().__init__(
            f"tenant {tenant_id} has {remaining} tokens of budget remaining, "
            f"below the {MIN_USEFUL_TOKENS} needed to serve a request"
        )
        self.tenant_id = tenant_id
        self.remaining = remaining


@dataclass(slots=True)
class Reservation:
    """Tokens held against a tenant's balance for one in-flight request."""

    tenant_id: str
    reserved: int
    effective_max_tokens: int
    requested_max_tokens: int
    actual: int | None = None
    settled: bool = False

    @property
    def clamped(self) -> bool:
        return self.effective_max_tokens < self.requested_max_tokens

    @property
    def outcome(self) -> BudgetOutcome:
        return BudgetOutcome.CLAMPED if self.clamped else BudgetOutcome.OK


@dataclass(slots=True)
class TenantBudget:
    limit: int | None
    spent: int = 0
    reserved: int = 0

    @property
    def available(self) -> int:
        if self.limit is None:
            return 2**62                       # effectively unlimited
        return max(0, self.limit - self.spent - self.reserved)


@dataclass(slots=True)
class BudgetLedger:
    """Per-tenant token accounting.

    In-process and single-writer, like the scheduler. Every mutation happens on
    the event loop thread between awaits, so the read-modify-write sequences
    below cannot interleave.
    """

    budgets: dict[str, TenantBudget] = field(default_factory=dict)

    @classmethod
    def from_tenants(cls, tenants: tuple[Tenant, ...]) -> BudgetLedger:
        return cls({t.id: TenantBudget(limit=t.budget_tokens) for t in tenants})

    def _budget(self, tenant_id: str) -> TenantBudget:
        budget = self.budgets.get(tenant_id)
        if budget is None:
            budget = self.budgets[tenant_id] = TenantBudget(limit=None)
        return budget

    def remaining(self, tenant_id: str) -> int:
        return self._budget(tenant_id).available

    def has_headroom(self, tenant_id: str) -> bool:
        """Cheap non-binding check, used before a request is allowed to queue.

        Spending an admission slot and a place in the queue on a request that
        cannot be paid for wastes the queue for everyone else.
        """
        budget = self._budget(tenant_id)
        return budget.limit is None or budget.available >= MIN_USEFUL_TOKENS

    @contextmanager
    def reserve(self, tenant_id: str, requested_max_tokens: int) -> Iterator[Reservation]:
        """Hold budget for one request, releasing whatever it did not use.

        Like the capacity lease, this is a context manager so that completion,
        provider failure, client disconnect and unexpected exceptions all settle
        the same way. A reservation that has to be released by hand is one that
        eventually is not, and an un-released reservation permanently shrinks a
        tenant's usable budget.
        """
        budget = self._budget(tenant_id)
        available = budget.available
        if budget.limit is not None and available < MIN_USEFUL_TOKENS:
            raise BudgetExceeded(tenant_id, available)

        effective = (
            requested_max_tokens if budget.limit is None
            else min(requested_max_tokens, available)
        )
        reservation = Reservation(
            tenant_id=tenant_id,
            reserved=effective,
            effective_max_tokens=effective,
            requested_max_tokens=requested_max_tokens,
        )
        budget.reserved += effective
        try:
            yield reservation
        finally:
            self._settle(reservation)

    def _settle(self, reservation: Reservation) -> None:
        if reservation.settled:
            return
        reservation.settled = True
        budget = self._budget(reservation.tenant_id)
        budget.reserved -= reservation.reserved

        # A cancelled or failed request that produced nothing costs nothing.
        # `actual` is clamped to the reservation because the reservation was the
        # ceiling: exceeding it would mean the clamp was not applied.
        actual = min(reservation.actual or 0, reservation.reserved)
        budget.spent += actual

    def snapshot(self) -> dict[str, dict[str, int | None]]:
        return {
            tid: {
                "limit": b.limit,
                "spent": b.spent,
                "reserved": b.reserved,
                "available": None if b.limit is None else b.available,
            }
            for tid, b in self.budgets.items()
        }

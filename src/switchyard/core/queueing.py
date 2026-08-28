"""Queue ordering policies.

Two policies, chosen because they answer the same question differently and the
difference is the point of the project: when capacity frees up, which waiting
request goes next?

FIFO is the baseline. It is what a gateway does when it does not think about
fairness, and it has a specific failure mode worth demonstrating rather than
merely asserting -- one tenant offering load faster than the others pushes their
requests arbitrarily far back in a shared queue.

Weighted fair queueing is the real policy. Each tenant carries a virtual clock
that advances by `cost / weight` on every dispatch, and the scheduler always
serves whichever backlogged tenant's clock is furthest behind. A tenant with
twice the weight advances at half the rate and therefore gets served twice as
often; a tenant issuing requests that cost ten times as much advances ten times
faster and is served correspondingly less.

Cost is measured in *tokens*, not requests, and that choice is the substance of
the policy. A tenant sending 4000-token completions consumes roughly ten times
the capacity of one sending 400-token completions at the same request rate.
Request-count fairness would call that situation fair. It is not.
"""

from __future__ import annotations

import itertools
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

_sequence = itertools.count()


@dataclass(slots=True)
class Waiter:
    """A request waiting for capacity."""

    tenant_id: str
    cost: float
    deadline_at: float
    enqueued_at: float
    seq: int = field(default_factory=lambda: next(_sequence))
    cancelled: bool = False

    def __hash__(self) -> int:
        return hash(self.seq)


Eligible = Callable[[str], bool]
"""Whether a tenant may currently take a slot (it may be at its own ceiling)."""


class QueuePolicy(Protocol):
    def enqueue(self, waiter: Waiter) -> None: ...
    def take_next(self, eligible: Eligible) -> Waiter | None: ...
    def remove(self, waiter: Waiter) -> None: ...
    def depth(self, tenant_id: str | None = None) -> int: ...
    def expired(self, now: float) -> list[Waiter]: ...
    def settle(self, tenant_id: str, estimated: float, actual: float) -> None: ...


class _QueueBase:
    """Shared bookkeeping: per-tenant deques plus expiry scanning."""

    def __init__(self) -> None:
        self._queues: dict[str, deque[Waiter]] = {}

    def enqueue(self, waiter: Waiter) -> None:
        self._queues.setdefault(waiter.tenant_id, deque()).append(waiter)

    def remove(self, waiter: Waiter) -> None:
        """Drop a waiter that gave up (deadline or client disconnect).

        Marked rather than searched-and-removed on the hot path: a cancelled
        waiter is skipped when it reaches the head, which keeps removal O(1)
        instead of O(queue length) for something that happens on every timeout.
        """
        waiter.cancelled = True

    def depth(self, tenant_id: str | None = None) -> int:
        if tenant_id is not None:
            return sum(1 for w in self._queues.get(tenant_id, ()) if not w.cancelled)
        return sum(1 for q in self._queues.values() for w in q if not w.cancelled)

    def expired(self, now: float) -> list[Waiter]:
        """Waiters past their deadline, removed from the queue."""
        out: list[Waiter] = []
        for queue in self._queues.values():
            keep: deque[Waiter] = deque()
            for waiter in queue:
                if waiter.cancelled:
                    continue
                if waiter.deadline_at <= now:
                    waiter.cancelled = True
                    out.append(waiter)
                else:
                    keep.append(waiter)
            queue.clear()
            queue.extend(keep)
        return out

    def settle(self, tenant_id: str, estimated: float, actual: float) -> None:
        """Correct a tenant's accounting once the true cost is known.

        Overridden where the policy tracks consumption. FIFO does not, so this
        is a no-op there.
        """

    def _head(self, tenant_id: str) -> Waiter | None:
        queue = self._queues.get(tenant_id)
        while queue:
            if queue[0].cancelled:
                queue.popleft()
                continue
            return queue[0]
        return None


class FifoQueue(_QueueBase):
    """Strict arrival order across all tenants. The comparison baseline."""

    def take_next(self, eligible: Eligible) -> Waiter | None:
        # Arrival order is the sequence number, so the global head is the
        # smallest seq among all tenants' heads.
        best: Waiter | None = None
        for tenant_id in self._queues:
            head = self._head(tenant_id)
            if head is None or not eligible(tenant_id):
                continue
            if best is None or head.seq < best.seq:
                best = head
        if best is not None:
            self._queues[best.tenant_id].popleft()
        return best


class WeightedFairQueue(_QueueBase):
    """Weighted fair queueing over per-tenant virtual clocks.

    `virtual_time[t]` advances by `cost / weight` on each dispatch; the tenant
    with the smallest clock is served next.
    """

    def __init__(self, weights: dict[str, float]) -> None:
        super().__init__()
        self._weights = weights
        self._virtual_time: dict[str, float] = {}

    def enqueue(self, waiter: Waiter) -> None:
        # A tenant that has been idle must not accumulate credit while away and
        # then monopolise the gateway on return. Rejoining at the current
        # minimum gives it its fair share from now on, and no more.
        if waiter.tenant_id not in self._virtual_time or self.depth(waiter.tenant_id) == 0:
            floor = min(self._virtual_time.values(), default=0.0) if self._backlogged() else 0.0
            self._virtual_time[waiter.tenant_id] = max(
                self._virtual_time.get(waiter.tenant_id, 0.0), floor
            )
        super().enqueue(waiter)

    def _backlogged(self) -> list[str]:
        return [t for t in self._queues if self._head(t) is not None]

    def take_next(self, eligible: Eligible) -> Waiter | None:
        best: Waiter | None = None
        best_key: tuple[float, int] | None = None

        for tenant_id in self._queues:
            head = self._head(tenant_id)
            if head is None or not eligible(tenant_id):
                continue
            # Sequence number breaks ties so ordering is deterministic, which
            # matters for reproducible tests.
            key = (self._virtual_time.get(tenant_id, 0.0), head.seq)
            if best_key is None or key < best_key:
                best, best_key = head, key

        if best is None:
            return None

        weight = self._weights.get(best.tenant_id, 1.0)
        self._virtual_time[best.tenant_id] = (
            self._virtual_time.get(best.tenant_id, 0.0) + best.cost / weight
        )
        self._queues[best.tenant_id].popleft()
        return best

    def settle(self, tenant_id: str, estimated: float, actual: float) -> None:
        """Charge the difference between predicted and real consumption.

        Scheduling has to commit before the cost is known, so the virtual clock
        advances on an estimate. Left uncorrected, a tenant whose requests are
        systematically longer than predicted would be under-charged on every
        single one and would drift into a permanently larger share. Settling
        makes the estimate provisional: fairness ends up measured against what
        was actually consumed, and the predictor's accuracy affects only how
        quickly the books balance, not whether they do.
        """
        if tenant_id not in self._virtual_time:
            return
        weight = self._weights.get(tenant_id, 1.0)
        self._virtual_time[tenant_id] += (actual - estimated) / weight

    def virtual_times(self) -> dict[str, float]:
        """Exposed for tests and for the fairness panel; not used for decisions."""
        return dict(self._virtual_time)


def build_policy(name: str, weights: dict[str, float]) -> QueuePolicy:
    if name == "fifo":
        return FifoQueue()
    if name == "drr":
        return WeightedFairQueue(weights)
    raise ValueError(f"unknown scheduling policy {name!r}")

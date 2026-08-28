"""Admission control and capacity scheduling.

The scarce resource is concurrent in-flight requests to providers. A request
holds a slot from dispatch until its stream ends, and how long that takes is not
knowable in advance -- it is proportional to output length, which the caller does
not declare and the model does not promise. Everything here follows from that.

Capacity model
--------------
Total capacity splits into a reserved pool and a shared pool:

    shared_capacity = max_concurrency - sum(tenant.reserved_concurrency)

A tenant below its own `reserved_concurrency` always has a slot available; that
floor is why a noisy neighbour cannot starve it. Above the floor it competes for
the shared pool, ordered by the queue policy and capped by its
`max_concurrency`. Configuration validation refuses reserved totals above the
gateway's capacity, so every floor can be honoured simultaneously.

Leases
------
Capacity is handed out as a lease held by an async context manager. This is the
whole defence against capacity leaks: completion, provider failure, client
disconnect, and unexpected exceptions all exit the block, and all release. A
release path that has to be remembered at each call site is a release path that
eventually is not.

No fast path
------------
Even when capacity is free, a request is enqueued and then dispatched by the
same pump that serves queued work. A separate uncontended fast path would be a
second admission mechanism with its own ordering semantics, and it would let a
newly arrived request overtake a queued one whenever a slot happened to be free.
The cost is one already-resolved future per request.
"""

from __future__ import annotations

import asyncio
import enum
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from switchyard.core.config import GatewayConfig, Tenant
from switchyard.core.queueing import QueuePolicy, Waiter, build_policy


class RejectReason(enum.StrEnum):
    QUEUE_FULL = "queue_full"
    DEADLINE = "deadline"
    SHUTTING_DOWN = "shutting_down"


class AdmissionRejected(Exception):
    """The request was refused before consuming any provider capacity."""

    def __init__(self, reason: RejectReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason
        self.message = message


@dataclass(slots=True)
class Lease:
    """A held slot. Released exactly once, by the context manager that made it."""

    tenant_id: str
    cost: float
    from_shared: bool
    queue_wait_s: float
    acquired_at: float
    released: bool = False
    actual_tokens: int | None = field(default=None)


@dataclass(slots=True)
class SchedulerStats:
    inflight: int
    shared_inflight: int
    shared_capacity: int
    queue_depth: int
    per_tenant_inflight: dict[str, int]
    per_tenant_queue_depth: dict[str, int]


class Scheduler:
    """Allocates provider concurrency across tenants."""

    def __init__(
        self,
        config: GatewayConfig,
        clock: Callable[[], float] = time.monotonic,
        on_dispatch: Callable[[Lease], None] | None = None,
        on_reject: Callable[[str, RejectReason], None] | None = None,
    ) -> None:
        self._tenants = config.tenants_by_id
        self._max_concurrency = config.max_concurrency
        self._clock = clock
        self._on_dispatch = on_dispatch
        self._on_reject = on_reject

        reserved_total = sum(t.reserved_concurrency for t in config.tenants)
        self._shared_capacity = config.max_concurrency - reserved_total

        self._policy: QueuePolicy = build_policy(
            config.scheduling_policy, {t.id: t.weight for t in config.tenants}
        )
        self._tenant_inflight: dict[str, int] = {}
        self._shared_inflight = 0
        self._waiting: dict[int, asyncio.Future[Lease]] = {}
        self._closed = False

    # -- capacity ---------------------------------------------------------

    def _inflight(self, tenant_id: str) -> int:
        return self._tenant_inflight.get(tenant_id, 0)

    def _eligible(self, tenant_id: str) -> bool:
        """Whether this tenant could take a slot right now."""
        tenant = self._tenants[tenant_id]
        inflight = self._inflight(tenant_id)
        if tenant.max_concurrency is not None and inflight >= tenant.max_concurrency:
            return False
        if inflight < tenant.reserved_concurrency:
            return True                                   # its own floor
        return self._shared_inflight < self._shared_capacity

    def _allocate(self, tenant_id: str) -> bool:
        """Take a slot. Returns whether it came from the shared pool."""
        tenant = self._tenants[tenant_id]
        from_shared = self._inflight(tenant_id) >= tenant.reserved_concurrency
        if from_shared:
            self._shared_inflight += 1
        self._tenant_inflight[tenant_id] = self._inflight(tenant_id) + 1
        return from_shared

    def _release(self, lease: Lease) -> None:
        if lease.released:
            return                                        # idempotent by design
        lease.released = True

        # Fairness is settled against real consumption, not the estimate the
        # scheduling decision was made on. See QueuePolicy.settle.
        if lease.actual_tokens is not None:
            self._policy.settle(lease.tenant_id, lease.cost, float(lease.actual_tokens))
        if lease.from_shared:
            self._shared_inflight -= 1
        remaining = self._inflight(lease.tenant_id) - 1
        if remaining:
            self._tenant_inflight[lease.tenant_id] = remaining
        else:
            self._tenant_inflight.pop(lease.tenant_id, None)
        self._pump()

    # -- dispatch ---------------------------------------------------------

    def _pump(self) -> None:
        """Dispatch as many queued requests as capacity and policy allow."""
        for stale in self._policy.expired(self._clock()):
            self._fail(stale, RejectReason.DEADLINE,
                       "queued longer than the request deadline")

        while not self._closed:
            waiter = self._policy.take_next(self._eligible)
            if waiter is None:
                return

            future = self._waiting.pop(waiter.seq, None)
            if future is None or future.done():
                continue                                  # caller already gave up

            from_shared = self._allocate(waiter.tenant_id)
            lease = Lease(
                tenant_id=waiter.tenant_id,
                cost=waiter.cost,
                from_shared=from_shared,
                queue_wait_s=self._clock() - waiter.enqueued_at,
                acquired_at=self._clock(),
            )
            future.set_result(lease)
            if self._on_dispatch:
                self._on_dispatch(lease)

    def _fail(self, waiter: Waiter, reason: RejectReason, message: str) -> None:
        future = self._waiting.pop(waiter.seq, None)
        if future is not None and not future.done():
            future.set_exception(AdmissionRejected(reason, message))

    def _reject(self, tenant_id: str, reason: RejectReason, message: str) -> AdmissionRejected:
        if self._on_reject:
            self._on_reject(tenant_id, reason)
        return AdmissionRejected(reason, message)

    # -- public API -------------------------------------------------------

    @asynccontextmanager
    async def acquire(self, tenant: Tenant, cost: float) -> AsyncIterator[Lease]:
        """Hold a capacity slot for the duration of the block.

        Raises AdmissionRejected before entering if the request cannot be
        admitted. Once entered, the slot is released on every exit path.
        """
        lease = await self._admit(tenant, cost)
        try:
            yield lease
        finally:
            self._release(lease)

    async def _admit(self, tenant: Tenant, cost: float) -> Lease:
        if tenant.id not in self._tenants:
            # Would otherwise surface as a KeyError inside the dispatch loop,
            # long after the mistake was made and with no useful context.
            raise KeyError(
                f"tenant {tenant.id!r} is not known to the scheduler; it must be "
                f"present in the configuration the scheduler was built from"
            )
        if self._closed:
            raise self._reject(tenant.id, RejectReason.SHUTTING_DOWN, "gateway is shutting down")

        # Bounded queue. Without this, overload turns into unbounded memory and
        # requests that complete long after the client stopped waiting.
        if self._policy.depth(tenant.id) >= tenant.max_queue_depth:
            raise self._reject(
                tenant.id, RejectReason.QUEUE_FULL,
                f"queue for tenant {tenant.id} is full "
                f"({tenant.max_queue_depth} waiting); retry later",
            )

        now = self._clock()
        waiter = Waiter(
            tenant_id=tenant.id, cost=cost,
            deadline_at=now + tenant.deadline_s, enqueued_at=now,
        )
        future: asyncio.Future[Lease] = asyncio.get_running_loop().create_future()
        self._waiting[waiter.seq] = future
        self._policy.enqueue(waiter)
        self._pump()

        try:
            return await asyncio.wait_for(future, timeout=tenant.deadline_s)
        except TimeoutError:
            self._abandon(waiter, future)
            raise self._reject(
                tenant.id, RejectReason.DEADLINE,
                f"waited longer than deadline_s={tenant.deadline_s:g} for capacity",
            ) from None
        except asyncio.CancelledError:
            # Client disconnected while queued.
            self._abandon(waiter, future)
            raise

    def _abandon(self, waiter: Waiter, future: asyncio.Future[Lease]) -> None:
        """Give up on a waiter, returning any slot granted in the same tick.

        The race is real: the pump can resolve the future in the same event-loop
        iteration that the timeout fires. Without this the slot would be
        allocated to a caller that has already gone, and never released.
        """
        self._policy.remove(waiter)
        self._waiting.pop(waiter.seq, None)
        if future.done() and not future.cancelled() and future.exception() is None:
            self._release(future.result())

    async def close(self) -> None:
        """Stop admitting and fail everything still queued."""
        self._closed = True
        for seq, future in list(self._waiting.items()):
            if not future.done():
                future.set_exception(
                    AdmissionRejected(RejectReason.SHUTTING_DOWN, "gateway is shutting down")
                )
            self._waiting.pop(seq, None)

    def stats(self) -> SchedulerStats:
        return SchedulerStats(
            inflight=sum(self._tenant_inflight.values()),
            shared_inflight=self._shared_inflight,
            shared_capacity=self._shared_capacity,
            queue_depth=self._policy.depth(),
            per_tenant_inflight=dict(self._tenant_inflight),
            per_tenant_queue_depth={
                t: self._policy.depth(t) for t in self._tenants
            },
        )

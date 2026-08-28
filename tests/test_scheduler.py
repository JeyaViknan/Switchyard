"""Scheduler capacity, admission, and lease safety.

The property that matters most here is that capacity is never leaked. A leaked
slot is permanent: the gateway's usable concurrency drops and never recovers,
and the symptom is a slow degradation that looks like a provider problem. Every
exit path from a lease gets its own test.
"""

from __future__ import annotations

import asyncio

import pytest

from switchyard.core.config import GatewayConfig, Tenant
from switchyard.core.scheduler import AdmissionRejected, RejectReason, Scheduler


def tenant(tid: str, **over) -> Tenant:
    return Tenant(**({"id": tid, "key_sha256": "a" * 64} | over))


def scheduler(max_concurrency: int = 4, policy: str = "drr", *tenants: Tenant) -> Scheduler:
    tenants = tenants or (tenant("t1"),)
    config = GatewayConfig(
        max_concurrency=max_concurrency, scheduling_policy=policy, tenants=tenants
    )
    config.validate()
    return Scheduler(config)


async def hold(sched: Scheduler, t: Tenant, release: asyncio.Event, cost: float = 100.0):
    async with sched.acquire(t, cost):
        await release.wait()


# -- capacity limits -------------------------------------------------------


async def test_global_capacity_is_never_exceeded():
    t = tenant("t1")
    sched = scheduler(3, "drr", t)
    release = asyncio.Event()
    peak = 0

    async def run():
        nonlocal peak
        async with sched.acquire(t, 100.0):
            peak = max(peak, sched.stats().inflight)
            await release.wait()

    tasks = [asyncio.create_task(run()) for _ in range(12)]
    await asyncio.sleep(0.05)
    assert sched.stats().inflight == 3
    release.set()
    await asyncio.gather(*tasks)
    assert peak <= 3
    assert sched.stats().inflight == 0


async def test_tenant_ceiling_is_enforced():
    t = tenant("t1", max_concurrency=2)
    sched = scheduler(8, "drr", t)
    release = asyncio.Event()

    tasks = [asyncio.create_task(hold(sched, t, release)) for _ in range(6)]
    await asyncio.sleep(0.05)
    assert sched.stats().per_tenant_inflight["t1"] == 2
    release.set()
    await asyncio.gather(*tasks)


async def test_reserved_floor_survives_a_greedy_neighbour():
    """The core multi-tenancy guarantee.

    `greedy` has no reservation, so every slot it takes comes from the shared
    pool and it can never occupy more than the shared pool holds. `protected`'s
    floor is therefore always reachable, however much load greedy offers.
    """
    greedy = tenant("greedy")                                  # reserved 0
    protected = tenant("protected", reserved_concurrency=2)
    sched = scheduler(4, "drr", greedy, protected)             # shared pool = 2

    release = asyncio.Event()
    flood = [asyncio.create_task(hold(sched, greedy, release)) for _ in range(20)]
    await asyncio.sleep(0.05)
    assert sched.stats().per_tenant_inflight["greedy"] == 2, "greedy is capped at the shared pool"

    later = [asyncio.create_task(hold(sched, protected, release)) for _ in range(2)]
    await asyncio.sleep(0.05)
    assert sched.stats().per_tenant_inflight["protected"] == 2, "floor reachable despite flood"

    release.set()
    await asyncio.gather(*flood, *later)
    assert sched.stats().inflight == 0


# -- lease release on every path -------------------------------------------


async def test_capacity_is_released_on_normal_completion():
    t = tenant("t1")
    sched = scheduler(1, "drr", t)
    async with sched.acquire(t, 100.0):
        assert sched.stats().inflight == 1
    assert sched.stats().inflight == 0


async def test_capacity_is_released_when_the_body_raises():
    t = tenant("t1")
    sched = scheduler(1, "drr", t)
    with pytest.raises(RuntimeError):
        async with sched.acquire(t, 100.0):
            raise RuntimeError("provider exploded")
    assert sched.stats().inflight == 0


async def test_capacity_is_released_when_the_holder_is_cancelled():
    t = tenant("t1")
    sched = scheduler(1, "drr", t)
    started = asyncio.Event()

    async def holder():
        async with sched.acquire(t, 100.0):
            started.set()
            await asyncio.sleep(3600)

    task = asyncio.create_task(holder())
    await started.wait()
    assert sched.stats().inflight == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sched.stats().inflight == 0


async def test_a_waiter_cancelled_while_queued_leaves_no_trace():
    t = tenant("t1")
    sched = scheduler(1, "drr", t)
    release = asyncio.Event()

    holder = asyncio.create_task(hold(sched, t, release))
    await asyncio.sleep(0.02)

    queued = asyncio.create_task(hold(sched, t, release))
    await asyncio.sleep(0.02)
    assert sched.stats().queue_depth == 1

    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert sched.stats().queue_depth == 0

    release.set()
    await holder
    assert sched.stats().inflight == 0


async def test_no_capacity_leaks_across_a_mixed_workload():
    """Completion, failure and cancellation interleaved; capacity must return to zero."""
    t = tenant("t1", max_queue_depth=1000, deadline_s=10.0)
    sched = scheduler(4, "drr", t)

    async def ok():
        async with sched.acquire(t, 100.0):
            await asyncio.sleep(0.005)

    async def boom():
        with pytest.raises(ValueError):
            async with sched.acquire(t, 100.0):
                raise ValueError

    async def vanish():
        async with sched.acquire(t, 100.0):
            await asyncio.sleep(3600)

    tasks = []
    for i in range(60):
        tasks.append(asyncio.create_task([ok, boom, vanish][i % 3]()))
    await asyncio.sleep(0.15)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(0.05)

    stats = sched.stats()
    assert stats.inflight == 0, f"leaked {stats.inflight} slots"
    assert stats.shared_inflight == 0
    assert stats.queue_depth == 0


# -- rejection -------------------------------------------------------------


async def test_a_full_queue_is_rejected_rather_than_grown():
    t = tenant("t1", max_queue_depth=2, deadline_s=5.0)
    sched = scheduler(1, "drr", t)
    release = asyncio.Event()

    running = [asyncio.create_task(hold(sched, t, release))]
    await asyncio.sleep(0.02)
    queued = [asyncio.create_task(hold(sched, t, release)) for _ in range(2)]
    await asyncio.sleep(0.02)

    with pytest.raises(AdmissionRejected) as exc:
        async with sched.acquire(t, 100.0):
            pass
    assert exc.value.reason is RejectReason.QUEUE_FULL
    assert "full" in exc.value.message

    release.set()
    await asyncio.gather(*running, *queued)


async def test_waiting_past_the_deadline_is_rejected():
    t = tenant("t1", deadline_s=0.05, max_queue_depth=10)
    sched = scheduler(1, "drr", t)
    release = asyncio.Event()
    holder = asyncio.create_task(hold(sched, t, release))
    await asyncio.sleep(0.02)

    with pytest.raises(AdmissionRejected) as exc:
        async with sched.acquire(t, 100.0):
            pass
    assert exc.value.reason is RejectReason.DEADLINE

    release.set()
    await holder
    assert sched.stats().queue_depth == 0
    assert sched.stats().inflight == 0


async def test_rejection_happens_before_any_capacity_is_consumed():
    t = tenant("t1", max_queue_depth=0)
    sched = scheduler(4, "drr", t)
    with pytest.raises(AdmissionRejected):
        async with sched.acquire(t, 100.0):
            pass
    assert sched.stats().inflight == 0


async def test_close_fails_queued_requests_instead_of_hanging_them():
    t = tenant("t1", max_queue_depth=10, deadline_s=30.0)
    sched = scheduler(1, "drr", t)
    release = asyncio.Event()

    holder = asyncio.create_task(hold(sched, t, release))
    await asyncio.sleep(0.02)
    queued = [asyncio.create_task(hold(sched, t, release)) for _ in range(3)]
    await asyncio.sleep(0.02)

    await sched.close()
    results = await asyncio.gather(*queued, return_exceptions=True)
    assert all(isinstance(r, AdmissionRejected) for r in results)
    assert all(r.reason is RejectReason.SHUTTING_DOWN for r in results)

    release.set()
    await holder


# -- callbacks and stats ---------------------------------------------------


async def test_queue_wait_is_reported_on_the_lease():
    t = tenant("t1", deadline_s=5.0)
    sched = scheduler(1, "drr", t)
    release = asyncio.Event()
    waits: list[float] = []

    async def record():
        async with sched.acquire(t, 100.0) as lease:
            waits.append(lease.queue_wait_s)

    holder = asyncio.create_task(hold(sched, t, release))
    await asyncio.sleep(0.02)
    queued = asyncio.create_task(record())
    await asyncio.sleep(0.08)
    release.set()
    await asyncio.gather(holder, queued)

    assert waits and waits[0] >= 0.05, "a request that queued should report the wait"


async def test_uncontended_requests_report_no_meaningful_queue_wait():
    t = tenant("t1")
    sched = scheduler(4, "drr", t)
    async with sched.acquire(t, 100.0) as lease:
        assert lease.queue_wait_s < 0.01

"""Guided demos that state a claim and then check it, live.

The CLI already has these scenarios. This is the same claims, driven from a
browser and scored while they run, so the verdict appears beside the thing it
describes rather than in a terminal afterwards.

Two rules shape the design:

**A check is pending until there is evidence.** Not "pass until proven
otherwise" -- a demo that shows green before anything has happened teaches the
audience to ignore it. Checks start pending, and only the evidence collected
during the measured phase can move them.

**Verdicts are measured, never scripted.** Each check reads the same snapshots
the live map is drawing from. If the gateway genuinely fails to protect the
quiet tenant, the console says FAIL in front of everyone. That is the point:
a demo that cannot fail is not a demonstration, it is an animation.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field
from typing import Any

from switchyard.console.fleet import FleetControl
from switchyard.console.traffic import TrafficGenerator
from switchyard.obs.logs import event, get_logger

log = get_logger("console.scenario")

PENDING, PASS, FAIL = "pending", "pass", "fail"

# The quiet tenant in the noisy-neighbour story, and the provider that dies in
# the outage story. Both are configuration in switchyard.toml; named here so
# the thresholds below can be read against them.
QUIET_TENANT = "globex"
NOISY_TENANT = "acme"
FAILING_PROVIDER = "fast"

# Thresholds. Each is the weakest claim still worth making: a floor that lets
# through 89% of a tenant's offered traffic has not protected it.
QUIET_SERVED_RATIO = 0.90
QUIET_QUEUE_WAIT_MS = 500.0
OUTAGE_SUCCESS_RATIO = 0.95

# How long the provider stays broken, and how long we will wait for the
# breaker to let it back in. Each failed probe doubles the cooldown, so a
# longer outage means a longer wait to see recovery -- 15s of failure is
# plenty to show the breaker open and traffic move, without backing the
# cooldown off so far that the recovery half of the story never arrives.
OUTAGE_S = 15.0
RECOVERY_MAX_S = 40.0


@dataclass(slots=True)
class Check:
    name: str
    claim: str
    status: str = PENDING
    detail: str = ""

    def as_dict(self) -> dict[str, str]:
        return {"name": self.name, "claim": self.claim,
                "status": self.status, "detail": self.detail}

    def decide(self, ok: bool, detail: str) -> None:
        self.status = PASS if ok else FAIL
        self.detail = detail


@dataclass(slots=True)
class ScenarioRun:
    name: str
    headline: str
    phase: str = "starting"
    checks: list[Check] = field(default_factory=list)
    summary: str = ""
    started_at: float = field(default_factory=time.monotonic)
    finished: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "headline": self.headline,
            "phase": self.phase,
            "finished": self.finished,
            "elapsed_s": round(time.monotonic() - self.started_at, 1),
            "summary": self.summary,
            "checks": [c.as_dict() for c in self.checks],
        }

    def conclude(self) -> None:
        self.finished = True
        self.phase = "done"
        failed = [c for c in self.checks if c.status == FAIL]
        pending = [c for c in self.checks if c.status == PENDING]
        if failed:
            self.summary = (f"{len(failed)} of {len(self.checks)} checks failed: "
                            + "; ".join(c.name for c in failed))
        elif pending:
            self.summary = (f"{len(pending)} check(s) had no evidence to judge: "
                            + "; ".join(c.name for c in pending))
        else:
            self.summary = f"All {len(self.checks)} checks passed."


class ScenarioRunner:
    """Runs one guided demo at a time and scores it from live snapshots."""

    def __init__(self, traffic: TrafficGenerator, fleet: FleetControl,
                 snapshot_of: Any) -> None:
        self._traffic = traffic
        self._fleet = fleet
        self._snapshot_of = snapshot_of      # callable returning the latest snapshot
        self.run: ScenarioRun | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def state(self) -> dict[str, Any] | None:
        return self.run.as_dict() if self.run else None

    async def start(self, name: str) -> None:
        if name not in {"noisy-neighbour", "provider-outage"}:
            raise KeyError(name)
        await self.stop()
        event(log, "console.scenario_started", scenario=name)
        if name == "noisy-neighbour":
            self.run = _noisy_neighbour_run()
            self._task = asyncio.create_task(self._noisy_neighbour(self.run))
        else:
            self.run = _provider_outage_run()
            self._task = asyncio.create_task(self._provider_outage(self.run))

    async def stop(self) -> None:
        """Stop the scenario and undo everything it did."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        await self._traffic.stop()
        with contextlib.suppress(Exception):
            await self._fleet.clear(FAILING_PROVIDER)
        if self.run and not self.run.finished:
            self.run.phase = "stopped"
            self.run.summary = "Stopped before it finished."
            self.run.finished = True

    async def _sample(self, run: ScenarioRun, phase: str, seconds: float,
                      until: Any = None) -> list[dict[str, Any]]:
        """Hold a phase for `seconds`, collecting a snapshot each second.

        `until` ends the phase early once the thing it is waiting for has
        happened. Recovery uses it: the breaker's cooldown doubles on each
        repeat trip, so how long it takes to close depends on how long the
        outage lasted. A fixed wait would either cut the story short or make
        every run drag past the point where anything changes.
        """
        run.phase = phase
        samples: list[dict[str, Any]] = []
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            await asyncio.sleep(1.0)
            snap = self._snapshot_of()
            if snap:
                samples.append(snap)
                if until is not None and until(snap):
                    break
        return samples

    # ------------------------------------------------------ noisy neighbour

    async def _noisy_neighbour(self, run: ScenarioRun) -> None:
        try:
            await self._traffic.start("noisy-neighbour")
            baseline = await self._sample(run, "baseline", 10.0)
            flood = await self._sample(run, "flood", 30.0)
            self._score_noisy(run, baseline, flood)
            await self._sample(run, "recovery", 15.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:                          # noqa: BLE001
            run.summary = f"Scenario errored: {exc!r}"
            event(log, "console.scenario_failed", scenario=run.name, error=repr(exc))
        finally:
            with contextlib.suppress(Exception):
                await self._traffic.stop()
            if not run.finished:
                run.conclude()

    def _score_noisy(self, run: ScenarioRun, baseline: list[dict[str, Any]],
                     flood: list[dict[str, Any]]) -> None:
        served, offered = _tenant_totals(flood, QUIET_TENANT)
        by_name = {c.name: c for c in run.checks}

        if offered <= 0:
            by_name["quiet_served"].decide(
                False, f"no traffic observed for '{QUIET_TENANT}' during the flood")
        else:
            ratio = served / offered
            by_name["quiet_served"].decide(
                ratio >= QUIET_SERVED_RATIO,
                f"{ratio:.0%} of offered traffic served "
                f"(needs {QUIET_SERVED_RATIO:.0%})")

        waits = [w for s in flood
                 for t in s.get("tenants", [])
                 if t["id"] == QUIET_TENANT and (w := t.get("queue_wait_p95_ms")) is not None]
        if not waits:
            by_name["quiet_not_queued"].decide(False, "no queue-wait samples")
        else:
            worst = max(waits)
            by_name["quiet_not_queued"].decide(
                worst < QUIET_QUEUE_WAIT_MS,
                f"queue wait p95 peaked at {worst:.0f}ms "
                f"(needs under {QUIET_QUEUE_WAIT_MS:.0f}ms)")

        noisy_rejected = _sum_over(flood, NOISY_TENANT, "rejected_rps")
        quiet_rejected = _sum_over(flood, QUIET_TENANT, "rejected_rps")
        by_name["rejections_land_on_the_cause"].decide(
            noisy_rejected > 0 and quiet_rejected == 0,
            f"'{NOISY_TENANT}' turned away {noisy_rejected:.0f} req/s-samples, "
            f"'{QUIET_TENANT}' {quiet_rejected:.0f}"
            + ("" if noisy_rejected else " -- the flood never exceeded capacity"),
        )
        _ = baseline      # kept for the UI's before/after, not scored

    # ------------------------------------------------------ provider outage

    async def _provider_outage(self, run: ScenarioRun) -> None:
        try:
            await self._traffic.start("steady")
            await self._sample(run, "healthy", 10.0)

            await self._fleet.set_faults(FAILING_PROVIDER, {"error_rate": 1.0})
            outage = await self._sample(run, "outage", OUTAGE_S)

            await self._fleet.clear(FAILING_PROVIDER)
            # Ends as soon as the breaker closes. The wait has to be generous
            # because the cooldown doubles on repeat trips -- that backoff is
            # the breaker protecting a provider that keeps failing, and the
            # demo waits it out rather than pretending it is not there.
            recovery = await self._sample(
                run, "recovery", RECOVERY_MAX_S, until=_breaker_closed)

            self._score_outage(run, outage, recovery)
        except asyncio.CancelledError:
            raise
        except Exception as exc:                          # noqa: BLE001
            run.summary = f"Scenario errored: {exc!r}"
            event(log, "console.scenario_failed", scenario=run.name, error=repr(exc))
        finally:
            with contextlib.suppress(Exception):
                await self._fleet.clear(FAILING_PROVIDER)
            with contextlib.suppress(Exception):
                await self._traffic.stop()
            if not run.finished:
                run.conclude()

    def _score_outage(self, run: ScenarioRun, outage: list[dict[str, Any]],
                      recovery: list[dict[str, Any]]) -> None:
        by_name = {c.name: c for c in run.checks}

        ratios = [r for s in outage
                  if (r := (s.get("kpis") or {}).get("success_ratio_60s")) is not None]
        if not ratios:
            by_name["clients_kept_being_served"].decide(False, "no traffic observed")
        else:
            worst = min(ratios)
            by_name["clients_kept_being_served"].decide(
                worst >= OUTAGE_SUCCESS_RATIO,
                f"success stayed at or above {worst:.1%} "
                f"(needs {OUTAGE_SUCCESS_RATIO:.0%})")

        opened = any(p["state"] in {"open", "half_open"}
                     for s in outage for p in s.get("providers", [])
                     if p["name"] == FAILING_PROVIDER)
        by_name["breaker_opened"].decide(
            opened,
            f"breaker for '{FAILING_PROVIDER}' opened"
            if opened else
            f"breaker for '{FAILING_PROVIDER}' never opened, so every request "
            f"kept paying to rediscover the outage",
        )

        failovers = sum(f.get("rps", 0.0) for s in outage for f in s.get("failovers", []))
        by_name["traffic_failed_over"].decide(
            failovers > 0,
            f"failovers observed away from '{FAILING_PROVIDER}'"
            if failovers else "no failovers seen",
        )

        closed = any(_breaker_closed(s) for s in recovery)
        by_name["recovered"].decide(
            closed,
            "breaker closed again after the provider recovered"
            if closed else "breaker had not closed by the end of the window",
        )


def _breaker_closed(snapshot: dict[str, Any]) -> bool:
    return any(p["name"] == FAILING_PROVIDER and p["state"] == "closed"
               for p in snapshot.get("providers", []))


def _tenant_totals(samples: list[dict[str, Any]], tenant: str) -> tuple[float, float]:
    served = _sum_over(samples, tenant, "admitted_rps")
    offered = _sum_over(samples, tenant, "offered_rps")
    return served, offered


def _sum_over(samples: list[dict[str, Any]], tenant: str, field_name: str) -> float:
    return float(sum(
        float(t.get(field_name, 0.0) or 0.0)
        for s in samples for t in s.get("tenants", []) if t["id"] == tenant
    ))


def _noisy_neighbour_run() -> ScenarioRun:
    return ScenarioRun(
        name="noisy-neighbour",
        headline=(f"'{NOISY_TENANT}' floods at 25 req/s while '{QUIET_TENANT}' "
                  f"keeps sending 1 req/s."),
        checks=[
            Check("quiet_served",
                  f"'{QUIET_TENANT}' keeps being served through the flood"),
            Check("quiet_not_queued",
                  f"'{QUIET_TENANT}' is not made to wait behind it"),
            Check("rejections_land_on_the_cause",
                  "the tenant causing the overload is the one turned away"),
        ],
    )


def _provider_outage_run() -> ScenarioRun:
    return ScenarioRun(
        name="provider-outage",
        headline=f"'{FAILING_PROVIDER}' fails completely for 25 seconds, under live traffic.",
        checks=[
            Check("clients_kept_being_served", "clients keep getting answers"),
            Check("breaker_opened",
                  f"the gateway stops calling '{FAILING_PROVIDER}'"),
            Check("traffic_failed_over", "traffic moves to the standby provider"),
            Check("recovered", "the provider is let back in once it is healthy"),
        ],
    )

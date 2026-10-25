"""Polls every source once a second and turns totals into a live picture.

Design notes worth knowing before reading:

**One snapshot, many sources, no single point of failure.** Each poll fans out
to the gateway, the fleet, Prometheus and Grafana concurrently with short
timeouts. A source that fails marks *that service* down and leaves the rest of
the snapshot intact. A console that goes blank because Grafana is unreachable
is worse than no console, since the moment you most want to look at it is the
moment something is broken.

**Rates come from counter deltas, percentiles from bucket deltas.** See
`promtext`. The consequence here is that the first poll after startup has no
rates -- there is nothing to difference against -- and says so rather than
reporting zeros that look like silence.

**Events are derived, not logged.** Nothing in the gateway emits "the breaker
opened" to the console. The collector notices that a value changed between two
snapshots and writes the sentence. That keeps the gateway free of console
concerns, and it means the feed can say what a change *meant* ("traffic now
goes to 'slow'") rather than echoing a state name.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

from switchyard.console import promtext
from switchyard.console.fleet import FleetControl
from switchyard.console.settings import Settings
from switchyard.obs.logs import event, get_logger

log = get_logger("console")

# Short, because the loop runs every second: a source that cannot answer in a
# second is down as far as a live view is concerned, and waiting longer would
# stall every other panel behind the slow one.
POLL_TIMEOUT_S = 1.0
MAX_EVENTS = 200
RATE_WINDOW_S = 60.0
FAILOVER_WINDOW_S = 300.0

BREAKER_NAMES = {0.0: "closed", 1.0: "half_open", 2.0: "open"}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class Event:
    at: str
    kind: str
    severity: str        # info | warning | critical
    text: str

    def as_dict(self) -> dict[str, str]:
        return {"at": self.at, "kind": self.kind, "severity": self.severity,
                "text": self.text}


@dataclass(slots=True)
class Collector:
    settings: Settings
    client: httpx.AsyncClient

    snapshot: dict[str, Any] = field(default_factory=dict)
    _scrapes: deque[promtext.Scrape] = field(default_factory=deque)
    _history: deque[dict[str, Any]] = field(default_factory=deque)
    _events: deque[Event] = field(default_factory=lambda: deque(maxlen=MAX_EVENTS))
    _previous: dict[str, Any] = field(default_factory=dict)
    _task: asyncio.Task[None] | None = None

    # ---------------------------------------------------------------- fetch

    def _admin_headers(self) -> dict[str, str]:
        return ({"authorization": f"Bearer {self.settings.admin_key}"}
                if self.settings.admin_key else {})

    async def _get_json(self, url: str, headers: dict[str, str] | None = None) -> Any:
        response = await self.client.get(url, headers=headers or {},
                                         timeout=POLL_TIMEOUT_S)
        response.raise_for_status()
        return response.json()

    async def _get_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        response = await self.client.get(url, headers=headers or {},
                                         timeout=POLL_TIMEOUT_S)
        response.raise_for_status()
        return response.text

    async def _fleet_profiles(self) -> dict[str, Any]:
        """Profiles keyed by provider, unwrapped from the control response."""
        return await FleetControl(self.settings.fleet_url, self.client).profiles()

    async def _try(self, coro: Any) -> Any | None:
        """Run a fetch, returning None instead of raising.

        Every source is optional. The snapshot reports which ones answered.
        """
        try:
            return await coro
        except (TimeoutError, httpx.HTTPError, ValueError):
            return None

    # ----------------------------------------------------------------- poll

    async def poll_once(self) -> dict[str, Any]:
        """Fetch everything once and build a snapshot."""
        gw = self.settings.gateway_url
        admin = self._admin_headers()

        readyz, stats, providers, metrics_text, profiles, fleet_health, prom, graf = (
            await asyncio.gather(
                self._try(self._get_json(f"{gw}/readyz")),
                self._try(self._get_json(f"{gw}/v1/scheduler/stats", admin)),
                self._try(self._get_json(f"{gw}/v1/providers", admin)),
                self._try(self._get_text(f"{gw}/metrics", admin)),
                self._try(self._fleet_profiles()),
                self._try(self._get_json(f"{self.settings.fleet_url}/health")),
                self._try(self._get_json(f"{self.settings.prometheus_url}/-/ready"))
                if self.settings.prometheus_url else _none(),
                self._try(self._get_json(f"{self.settings.grafana_url}/api/health"))
                if self.settings.grafana_url else _none(),
            )
        )

        now = time.monotonic()
        scrape = promtext.parse(metrics_text, at=now) if metrics_text else None
        previous = self._scrapes[-1] if self._scrapes else None
        if scrape is not None:
            self._scrapes.append(scrape)
            self._trim(now)

        snapshot = self._build(
            readyz=readyz, stats=stats, providers=providers, scrape=scrape,
            previous=previous, profiles=profiles, fleet_up=fleet_health is not None,
            prometheus_up=prom is not None, grafana_up=graf is not None,
        )
        self._derive_events(snapshot)
        snapshot["events"] = [e.as_dict() for e in reversed(self._events)]
        self._remember_history(snapshot, now)
        self._previous = snapshot
        self.snapshot = snapshot
        return snapshot

    def _trim(self, now: float) -> None:
        horizon = now - self.settings.history_minutes * 60
        while len(self._scrapes) > 2 and self._scrapes[0].at < horizon:
            self._scrapes.popleft()
        while self._history and self._history[0]["t"] < horizon:
            self._history.popleft()

    def _scrape_at_least_ago(self, seconds: float) -> promtext.Scrape | None:
        """Oldest scrape within the window, for windowed ratios."""
        if not self._scrapes:
            return None
        cutoff = self._scrapes[-1].at - seconds
        for s in self._scrapes:
            if s.at >= cutoff:
                return s if s is not self._scrapes[-1] else None
        return None

    # ---------------------------------------------------------------- build

    def _build(
        self, *, readyz: Any, stats: Any, providers: Any,
        scrape: promtext.Scrape | None, previous: promtext.Scrape | None,
        profiles: Any, fleet_up: bool, prometheus_up: bool, grafana_up: bool,
    ) -> dict[str, Any]:
        gateway_up = readyz is not None or stats is not None
        draining = bool(readyz and readyz.get("status") == "draining")

        provider_rows = self._providers(providers, scrape, previous, profiles)
        # Simulation unless something is pointed at a real provider. Derived
        # from what the gateway reports, so it cannot drift from reality the
        # way a hand-set flag would.
        kinds = {p["kind"] for p in provider_rows}
        mode = "live-providers" if kinds - {"synthetic"} else "simulation"

        return {
            "at": _utc_now(),
            "mode": mode,
            "build": self._build_info(readyz),
            "services": {
                "gateway": "draining" if draining else ("ready" if gateway_up else "down"),
                "fleet": "up" if fleet_up else "down",
                "prometheus": self._optional(self.settings.prometheus_url, prometheus_up),
                "grafana": self._optional(self.settings.grafana_url, grafana_up),
            },
            "gateway": self._gateway(readyz, stats, draining),
            "tenants": self._tenants(stats, scrape, previous),
            "providers": provider_rows,
            "failovers": self._failovers(scrape, previous),
            "kpis": self._kpis(scrape, previous, stats),
            "demo_controls": self.settings.demo_controls_available,
        }

    @staticmethod
    def _optional(url: str, up: bool) -> str:
        if not url:
            return "not configured"
        return "up" if up else "down"

    def _build_info(self, readyz: Any) -> dict[str, Any]:
        info: dict[str, Any] = {
            "version": (readyz or {}).get("version"),
            "git_sha": (readyz or {}).get("git_sha") or self.settings.git_sha,
            "uptime_s": (readyz or {}).get("uptime_s"),
            "deployed_at": self.settings.deployed_at or None,
            "run_url": self.settings.deploy_run_url or None,
        }
        sha = info["git_sha"]
        info["short_sha"] = sha[:7] if isinstance(sha, str) else None
        return info

    @staticmethod
    def _gateway(readyz: Any, stats: Any, draining: bool) -> dict[str, Any]:
        stats = stats or {}
        pool = stats.get("shared_pool") or {}
        return {
            "policy": stats.get("policy") or (readyz or {}).get("policy"),
            "max_concurrency": stats.get("max_concurrency")
                               or (readyz or {}).get("max_concurrency"),
            "inflight": stats.get("inflight", (readyz or {}).get("inflight", 0)),
            "shared_pool": {"in_use": pool.get("in_use", 0),
                            "capacity": pool.get("capacity", 0)},
            "queue_depth": stats.get("queue_depth", 0),
            "draining": draining,
        }

    def _tenants(
        self, stats: Any, scrape: promtext.Scrape | None,
        previous: promtext.Scrape | None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for tid, t in sorted(((stats or {}).get("tenants") or {}).items()):
            admitted = (promtext.rate(previous, scrape, "switchyard_dispatched_total",
                                      tenant=tid) if scrape else 0.0)
            rejected = (promtext.rate_total(previous, scrape,
                                            "switchyard_admission_rejected_total",
                                            tenant=tid) if scrape else 0.0)
            tokens = (promtext.rate(previous, scrape, "switchyard_tenant_tokens_total",
                                    tenant=tid) if scrape else 0.0)
            budget = t.get("budget") or {}
            rows.append({
                "id": tid,
                "weight": t.get("weight"),
                "floor": t.get("reserved_concurrency"),
                "ceiling": t.get("max_concurrency"),
                "inflight": t.get("inflight", 0),
                "queued": t.get("queued", 0),
                # Offered is what the tenant asked for: everything admitted
                # plus everything turned away. Neither number alone shows a
                # tenant being shielded from its neighbour.
                "offered_rps": round(admitted + rejected, 2),
                "admitted_rps": round(admitted, 2),
                "rejected_rps": round(rejected, 2),
                "rejected_by_reason": self._rejections(scrape, previous, tid),
                "queue_wait_p95_ms": self._queue_wait_p95_ms(tid),
                "tokens_per_s": round(tokens, 1),
                "budget": {
                    "limit": budget.get("limit"),
                    "spent": budget.get("spent"),
                    "available": budget.get("available"),
                    "requests_remaining": budget.get("requests_remaining_estimate"),
                },
            })
        return rows

    @staticmethod
    def _rejections(
        scrape: promtext.Scrape | None, previous: promtext.Scrape | None, tenant: str
    ) -> dict[str, float]:
        if scrape is None:
            return {}
        out: dict[str, float] = {}
        for key in scrape.counters.get("switchyard_admission_rejected_total", {}):
            labels = dict(key)
            if labels.get("tenant") != tenant:
                continue
            reason = labels.get("reason", "unknown")
            r = promtext.rate(previous, scrape, "switchyard_admission_rejected_total",
                              tenant=tenant, reason=reason)
            if r > 0:
                out[reason] = round(r, 2)
        return out

    def _queue_wait_p95_ms(self, tenant: str) -> float | None:
        """p95 over the rate window, recomputed from bucket deltas.

        Approximate: resolution is bounded by the histogram's bucket edges.
        The UI labels it as such rather than implying it is exact.
        """
        if not self._scrapes:
            return None
        current = self._scrapes[-1].histograms.get(
            "switchyard_queue_wait_seconds", {}).get(promtext.labels_of({"tenant": tenant}))
        if current is None:
            return None
        earlier = self._scrape_at_least_ago(RATE_WINDOW_S)
        before = (earlier.histograms.get("switchyard_queue_wait_seconds", {})
                  .get(promtext.labels_of({"tenant": tenant})) if earlier else None)
        seconds = promtext.quantile_from_buckets(before, current, q=0.95)
        return None if seconds is None else round(seconds * 1000, 1)

    def _providers(
        self, providers: Any, scrape: promtext.Scrape | None,
        previous: promtext.Scrape | None, profiles: Any,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        by_name = providers or {}
        profile_map = profiles or {}
        for name, p in sorted(by_name.items()):
            fault = self._fault_of(profile_map.get(name))
            rows.append({
                "name": name,
                "kind": p.get("kind", "synthetic"),
                "state": p.get("state", "closed"),
                "reopens_in_s": p.get("reopens_in_s"),
                "ttft_ewma_ms": p.get("ttft_ewma_ms"),
                "ok_rps": round(promtext.rate_total(
                    previous, scrape, "switchyard_requests_total",
                    provider=name, outcome="ok"), 2) if scrape else 0.0,
                "failed_rps": round(promtext.rate_total(
                    previous, scrape, "switchyard_requests_total",
                    provider=name, outcome="error"), 2) if scrape else 0.0,
                "fault": fault,
                "routes_serving": p.get("routes_serving", []),
            })
        return rows

    @staticmethod
    def _fault_of(profile: Any) -> dict[str, Any] | None:
        """What has been injected into this provider, if anything."""
        return FleetControl.active_faults(profile)

    def _failovers(
        self, scrape: promtext.Scrape | None, previous: promtext.Scrape | None
    ) -> list[dict[str, Any]]:
        if scrape is None:
            return []
        out: list[dict[str, Any]] = []
        for key in scrape.counters.get("switchyard_failovers_total", {}):
            labels = dict(key)
            frm, to = labels.get("from_provider"), labels.get("to_provider")
            if frm is None or to is None:
                continue
            r = promtext.rate(previous, scrape, "switchyard_failovers_total",
                              from_provider=frm, to_provider=to)
            if r > 0:
                out.append({"from": frm, "to": to, "rps": round(r, 2)})
        return out

    def _kpis(
        self, scrape: promtext.Scrape | None, previous: promtext.Scrape | None,
        stats: Any,
    ) -> dict[str, Any]:
        if scrape is None:
            return {"rps": 0.0, "success_ratio_60s": None, "failovers_5m": 0,
                    "capacity_utilisation": 0.0}

        rps = promtext.rate_total(previous, scrape, "switchyard_requests_total")

        # Ratios over a window, not per-poll: a single second can contain one
        # request, and "0% success" from one failure is noise, not signal.
        window = self._scrape_at_least_ago(RATE_WINDOW_S)
        ok = promtext.delta(window.counter_total("switchyard_requests_total", outcome="ok"),
                            scrape.counter_total("switchyard_requests_total", outcome="ok")
                            ) if window else 0.0
        total = promtext.delta(window.counter_total("switchyard_requests_total"),
                               scrape.counter_total("switchyard_requests_total")
                               ) if window else 0.0
        success = round(ok / total, 4) if total > 0 else None

        five_min = self._scrape_at_least_ago(FAILOVER_WINDOW_S)
        failovers = promtext.delta(
            five_min.counter_total("switchyard_failovers_total"),
            scrape.counter_total("switchyard_failovers_total"),
        ) if five_min else 0.0

        utilisation = scrape.gauge("switchyard_capacity_utilisation")
        if utilisation is None:
            inflight = (stats or {}).get("inflight", 0)
            cap = (stats or {}).get("max_concurrency") or 0
            utilisation = (inflight / cap) if cap else 0.0

        return {
            "rps": round(rps, 2),
            "success_ratio_60s": success,
            "failovers_5m": int(failovers),
            "capacity_utilisation": round(utilisation, 3),
        }

    # --------------------------------------------------------------- events

    def _emit(self, kind: str, severity: str, text: str) -> None:
        self._events.append(Event(at=_utc_now(), kind=kind, severity=severity, text=text))

    def _derive_events(self, snapshot: dict[str, Any]) -> None:
        """Say what changed, in a sentence, by comparing two snapshots."""
        before = self._previous
        if not before:
            return

        was = {p["name"]: p for p in before.get("providers", [])}
        for p in snapshot.get("providers", []):
            old = was.get(p["name"])
            if old and old["state"] != p["state"]:
                self._breaker_event(p, old["state"])
            if old and bool(old.get("fault")) != bool(p.get("fault")):
                if p.get("fault"):
                    self._emit("fault_injected", "warning",
                               f"Fault injected into '{p['name']}': {p['fault']}")
                else:
                    self._emit("fault_cleared", "info",
                               f"Fault cleared on '{p['name']}'")

        old_services = before.get("services", {})
        for name, state in snapshot.get("services", {}).items():
            was_state = old_services.get(name)
            if was_state and was_state != state:
                severity = "critical" if state == "down" else "info"
                if state == "draining":
                    severity = "warning"
                self._emit(f"{name}_{state}", severity,
                           f"{name.capitalize()} is {state} (was {was_state})")

        old_tenants = {t["id"]: t for t in before.get("tenants", [])}
        for t in snapshot.get("tenants", []):
            old = old_tenants.get(t["id"])
            if not old:
                continue
            if old["rejected_rps"] == 0 and t["rejected_rps"] > 0:
                self._emit("tenant_rejected", "warning",
                           f"Tenant '{t['id']}' is being turned away "
                           f"({t['rejected_rps']:.0f}/s) -- it is over its share")
            elif old["rejected_rps"] > 0 and t["rejected_rps"] == 0:
                self._emit("tenant_accepted", "info",
                           f"Tenant '{t['id']}' is being served again")
            self._budget_event(t, old)

        old_sha = (before.get("build") or {}).get("git_sha")
        new_sha = (snapshot.get("build") or {}).get("git_sha")
        if old_sha and new_sha and old_sha != new_sha:
            self._emit("deployed", "info",
                       f"New build is serving: {new_sha[:7]} (was {old_sha[:7]})")

    def _breaker_event(self, provider: dict[str, Any], was: str) -> None:
        name, state = provider["name"], provider["state"]
        if state == "open":
            fallbacks = [r for r in provider.get("routes_serving", [])]
            where = f"; traffic now goes to the next provider for {fallbacks}" if fallbacks else ""
            self._emit("breaker_opened", "warning",
                       f"Breaker for '{name}' opened after repeated failures{where}")
        elif state == "half_open":
            self._emit("breaker_probing", "info",
                       f"Breaker for '{name}' is probing to see if it recovered")
        elif was in {"open", "half_open"}:
            self._emit("breaker_closed", "info",
                       f"Breaker for '{name}' closed -- it is healthy again")

    def _budget_event(self, tenant: dict[str, Any], old: dict[str, Any]) -> None:
        budget, old_budget = tenant.get("budget") or {}, old.get("budget") or {}
        limit, available = budget.get("limit"), budget.get("available")
        was_available = old_budget.get("available")
        if not limit or available is None or was_available is None:
            return
        if available / limit < 0.1 <= was_available / limit:
            self._emit("budget_low", "warning",
                       f"Tenant '{tenant['id']}' is below 10% of its token budget")

    # -------------------------------------------------------------- history

    def _remember_history(self, snapshot: dict[str, Any], now: float) -> None:
        kpis = snapshot["kpis"]
        self._history.append({
            "t": now,
            "at": snapshot["at"],
            "rps": kpis["rps"],
            "capacity_utilisation": kpis["capacity_utilisation"],
            "success_ratio": kpis["success_ratio_60s"],
            "tenants": {t["id"]: {"admitted_rps": t["admitted_rps"],
                                  "rejected_rps": t["rejected_rps"],
                                  "queue_wait_p95_ms": t["queue_wait_p95_ms"]}
                        for t in snapshot["tenants"]},
        })

    def history(self, minutes: int = 10) -> list[dict[str, Any]]:
        if not self._history:
            return []
        cutoff = self._history[-1]["t"] - minutes * 60
        return [{k: v for k, v in point.items() if k != "t"}
                for point in self._history if point["t"] >= cutoff]

    # ------------------------------------------------------------- lifecycle

    async def run(self) -> None:
        """Poll forever. Never raises: a dead collector is a blank console."""
        while True:
            started = time.monotonic()
            try:
                await self.poll_once()
            except Exception as exc:                     # noqa: BLE001
                event(log, "console.poll_failed", error=repr(exc))
            # Drift-free: sleep what is left of the interval, not the whole of
            # it, so a slow poll does not push every later poll later still.
            remaining = self.settings.poll_interval_s - (time.monotonic() - started)
            await asyncio.sleep(max(0.05, remaining))

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None


async def _none() -> None:
    return None

"""`switchyard top` -- live view of what the gateway is doing right now.

Aggregate metrics answer "how has the gateway behaved"; this answers "what is it
doing at this instant", which is the question you actually have while watching a
noisy tenant arrive or a provider fail. Per-tenant queue depth diverging in real
time makes fair scheduling legible in a way a time series does not.

It is a plain HTTP client over the operational endpoints, with no privileged
access to gateway internals, so it works identically against a local process and
a remote deployment.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from switchyard.cli.render import (
    CLEAR_SCREEN,
    HIDE_CURSOR,
    SHOW_CURSOR,
    Style,
    bar,
    compact,
)

_SAMPLE = re.compile(r"^(?P<name>switchyard_[a-z_]+)(?:\{(?P<labels>[^}]*)\})? (?P<value>\S+)$")


def parse_metrics(text: str) -> dict[str, list[tuple[dict[str, str], float]]]:
    """Parse the Prometheus exposition format into name -> [(labels, value)]."""
    out: dict[str, list[tuple[dict[str, str], float]]] = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        match = _SAMPLE.match(line)
        if not match:
            continue
        labels = {}
        if raw := match.group("labels"):
            for part in raw.split('",'):
                if "=" in part:
                    key, _, value = part.partition("=")
                    labels[key.strip()] = value.strip().strip('"')
        with contextlib.suppress(ValueError):
            out.setdefault(match.group("name"), []).append((labels, float(match.group("value"))))
    return out


def total(metrics: dict, name: str, **match: str) -> float:
    return sum(
        value for labels, value in metrics.get(name, [])
        if all(labels.get(k) == v for k, v in match.items())
    )


def breakdown(metrics: dict, name: str, label: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for labels, value in metrics.get(name, []):
        key = labels.get(label, "?")
        out[key] = out.get(key, 0.0) + value
    return {k: v for k, v in sorted(out.items(), key=lambda kv: -kv[1]) if v}


@dataclass(slots=True)
class Snapshot:
    """One poll of the gateway."""

    stats: dict[str, Any]
    providers: dict[str, Any]
    metrics: dict[str, list[tuple[dict[str, str], float]]] = field(default_factory=dict)
    error: str | None = None


async def poll(client: httpx.AsyncClient, base_url: str, headers: dict[str, str]) -> Snapshot:
    try:
        stats, providers, metrics = await asyncio.gather(
            client.get(f"{base_url}/v1/scheduler/stats", headers=headers),
            client.get(f"{base_url}/v1/providers", headers=headers),
            client.get(f"{base_url}/metrics", headers=headers),
        )
    except httpx.HTTPError as exc:
        return Snapshot({}, {}, error=f"cannot reach {base_url}: {exc}")

    if stats.status_code == 401:
        return Snapshot({}, {}, error=(
            "unauthorised. Pass --key or set SWITCHYARD_ADMIN_KEY; mint one with "
            "`switchyard keys mint --admin`."
        ))
    if stats.status_code != 200:
        return Snapshot({}, {}, error=f"gateway returned {stats.status_code}: {stats.text[:200]}")

    return Snapshot(stats.json(), providers.json(), parse_metrics(metrics.text))


def render(snapshot: Snapshot, base_url: str, style: Style) -> str:
    if snapshot.error:
        return f"\n  {style.red('switchyard top')}  {snapshot.error}\n"

    stats, providers, metrics = snapshot.stats, snapshot.providers, snapshot.metrics
    capacity = stats["max_concurrency"]
    inflight = stats["inflight"]
    lines: list[str] = []

    lines.append(
        f"{style.bold('switchyard')} {style.dim(base_url)}   "
        f"policy {style.cyan(stats['policy'])}   "
        f"capacity {bar(inflight / capacity if capacity else 0, style=style)} "
        f"{inflight}/{capacity}   queued {stats['queue_depth']}"
    )
    lines.append("")

    lines.append(style.dim(
        f"  {'TENANT':<14}{'WEIGHT':>7}{'FLOOR':>7}{'INFLIGHT':>10}"
        f"{'QUEUED':>8}{'TOKENS':>10}{'BUDGET':>18}"
    ))
    for name, t in stats["tenants"].items():
        queued = t["queued"]
        budget = t.get("budget") or {}
        if budget.get("limit"):
            used = budget["spent"] / budget["limit"]
            budget_text = f"{compact(budget['available'])} / {compact(budget['limit'])} left"
            if used >= 0.9:
                budget_text = style.red(budget_text)
            elif used >= 0.7:
                budget_text = style.yellow(budget_text)
        else:
            budget_text = style.dim("unlimited")

        tokens = total(metrics, "switchyard_tenant_tokens_total", tenant=name)
        queued_text = style.yellow(f"{queued:>8}") if queued else style.dim(f"{queued:>8}")
        lines.append(
            f"  {name:<14}{t['weight']:>7.1f}{t['reserved_concurrency']:>7}"
            f"{t['inflight']:>10}{queued_text}{compact(tokens):>10}  {budget_text}"
        )

    lines.append("")
    lines.append(style.dim(
        f"  {'PROVIDER':<14}{'STATE':<12}{'OK':>8}{'FAILED':>8}{'TTFT':>9}   NOTE"
    ))
    for name, p in providers.items():
        state = p["state"]
        coloured = {
            "closed": style.green(f"{state:<12}"),
            "half_open": style.yellow(f"{state:<12}"),
            "open": style.red(f"{state:<12}"),
        }.get(state, f"{state:<12}")
        ttft = f"{p['ttft_ewma_ms']:.0f}ms" if p.get("ttft_ewma_ms") else "-"
        note = ""
        if p.get("reopens_in_s") is not None:
            note = style.dim(f"retries in {p['reopens_in_s']:.1f}s")
        elif p.get("errors"):
            note = style.dim(", ".join(f"{k}={int(v)}" for k, v in list(p["errors"].items())[:3]))
        lines.append(
            f"  {name:<14}{coloured}{int(p['successes']):>8}{int(p['failures']):>8}"
            f"{ttft:>9}   {note}"
        )

    rejects = breakdown(metrics, "switchyard_admission_rejected_total", "reason")
    footer = [
        f"failovers {int(total(metrics, 'switchyard_failovers_total'))}",
        f"skipped {int(total(metrics, 'switchyard_provider_skipped_total'))}",
        f"clamped {int(total(metrics, 'switchyard_max_tokens_clamped_total'))}",
    ]
    if rejects:
        detail = ", ".join(f"{k} {int(v)}" for k, v in rejects.items())
        footer.append(f"rejected {int(sum(rejects.values()))} ({detail})")
    lines.append("")
    lines.append(style.dim("  " + "  ·  ".join(footer)))
    return "\n".join(lines) + "\n"


async def run(base_url: str, key: str | None, interval: float, once: bool) -> int:
    headers = {"authorization": f"Bearer {key}"} if key else {}
    style = Style()
    async with httpx.AsyncClient(timeout=5.0) as client:
        if once:
            snapshot = await poll(client, base_url, headers)
            print(render(snapshot, base_url, style))
            return 1 if snapshot.error else 0

        print(HIDE_CURSOR, end="")
        try:
            while True:
                started = time.perf_counter()
                snapshot = await poll(client, base_url, headers)
                print(CLEAR_SCREEN + render(snapshot, base_url, style), end="", flush=True)
                await asyncio.sleep(max(0.0, interval - (time.perf_counter() - started)))
        except KeyboardInterrupt:
            return 0
        finally:
            print(SHOW_CURSOR, end="")

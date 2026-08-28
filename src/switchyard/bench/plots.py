"""Figure generation.

Every figure is produced from a stored run, never from live measurement, so a
plot can always be regenerated and audited against the data it came from.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402


def _save(fig, path: str) -> str:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fig.savefig(path, bbox_inches="tight", dpi=140)
    plt.close(fig)
    return path


def latency_vs_offered_load(
    rates: Sequence[float], p50: Sequence[float], p95: Sequence[float],
    p99: Sequence[float], throughput: Sequence[float], goodput: Sequence[float],
    path: str, title: str = "Baseline: passthrough gateway",
) -> str:
    """The shape every later scheduling change is compared against.

    Two panels because throughput alone hides congestion collapse: a saturated
    system can keep completing requests while every one of them arrives too late
    to be useful. Goodput is what separates those cases.
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2))

    ax1.plot(rates, np.asarray(p50) * 1000, "o-", label="p50")
    ax1.plot(rates, np.asarray(p95) * 1000, "s-", label="p95")
    ax1.plot(rates, np.asarray(p99) * 1000, "^-", label="p99")
    ax1.set_xlabel("offered load (req/s)")
    ax1.set_ylabel("end-to-end latency (ms)")
    ax1.set_title("Latency vs offered load")
    ax1.legend()
    ax1.grid(alpha=0.3)

    ax2.plot(rates, throughput, "o-", label="throughput")
    ax2.plot(rates, goodput, "s--", label="goodput (within SLO)")
    ax2.plot(rates, rates, ":", color="grey", label="offered")
    ax2.set_xlabel("offered load (req/s)")
    ax2.set_ylabel("completed (req/s)")
    ax2.set_title("Throughput vs goodput")
    ax2.legend()
    ax2.grid(alpha=0.3)

    fig.suptitle(title)
    return _save(fig, path)


def distribution(
    samples_by_label: dict[str, Sequence[float]], path: str,
    xlabel: str = "latency (ms)", title: str = "Distribution",
) -> str:
    """CDF rather than a histogram: percentiles are read directly off the curve,
    and tail behavior stays visible instead of being flattened into a bin."""
    fig, ax = plt.subplots(figsize=(6.5, 4.2))
    for label, samples in samples_by_label.items():
        if not len(samples):
            continue
        arr = np.sort(np.asarray(samples, dtype=float)) * 1000
        ax.plot(arr, np.linspace(0, 1, len(arr)), label=f"{label} (n={len(arr)})")
    for q in (0.5, 0.95, 0.99):
        ax.axhline(q, color="grey", ls=":", lw=0.8)
        ax.text(ax.get_xlim()[1], q, f" p{int(q * 100)}", va="center", fontsize=8, color="grey")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("cumulative fraction")
    ax.set_title(title)
    ax.legend()
    ax.grid(alpha=0.3)
    return _save(fig, path)


def fairness_comparison(results: dict, tenants, path: str) -> str:
    """Token share and queue wait per tenant, FIFO against weighted.

    Two panels because the benchmark asks two different questions. Share answers
    proportionality -- how contended capacity was divided. Queue wait answers
    isolation -- whether a tenant asking for little still gets served promptly
    while someone else floods the gateway. A policy can do well on one and badly
    on the other.
    """
    names = [t.tenant_id for t in tenants]
    policies = ["fifo", "drr"]
    labels = {"fifo": "FIFO (baseline)", "drr": "weighted fair"}
    colours = {"fifo": "#b0b0b0", "drr": "#3572b0"}

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.4))
    x = np.arange(len(names))
    width = 0.36

    for i, policy in enumerate(policies):
        shares = [results[policy]["token_share"][n] * 100 for n in names]
        ax1.bar(x + (i - 0.5) * width, shares, width,
                label=labels[policy], color=colours[policy])

    # Entitlement for the backlogged tenants, expressed on the same axis as the
    # bars. A tenant asking for less than its share takes what it wants first;
    # what remains is what the backlogged tenants are actually competing over,
    # and that is what weight divides. Drawing the line as a share of contended
    # capacity instead would make an on-target bar look short by exactly the
    # amount the light tenant consumed.
    backlogged = [t for t in tenants if t.backlogged]
    total_weight = sum(t.weight for t in backlogged)
    contended = 100.0 - sum(
        results["drr"]["token_share"][t.tenant_id] * 100
        for t in tenants if not t.backlogged
    )
    for t in backlogged:
        idx = names.index(t.tenant_id)
        target = t.weight / total_weight * contended
        ax1.plot([idx - 0.42, idx + 0.42], [target, target], "--", color="#c04040", lw=1.6,
                 label="weighted entitlement" if t is backlogged[0] else None)

    ax1.set_xticks(x)
    ax1.set_xticklabels(names)
    ax1.set_ylabel("share of output tokens (%)")
    ax1.set_title("Who got the capacity")
    ax1.legend(fontsize=8)
    ax1.grid(alpha=0.3, axis="y")

    for i, policy in enumerate(policies):
        waits = [results[policy]["tenants"][n]["queue_wait_p95_ms"] for n in names]
        ax2.bar(x + (i - 0.5) * width, waits, width,
                label=labels[policy], color=colours[policy])
    ax2.set_xticks(x)
    ax2.set_xticklabels(names)
    ax2.set_ylabel("queue wait p95 (ms)")
    ax2.set_yscale("symlog", linthresh=10)
    ax2.set_title("How long each tenant waited for capacity")
    ax2.legend(fontsize=8, loc="lower right")
    ax2.grid(alpha=0.3, axis="y")

    fig.suptitle("Multi-tenant fairness under sustained backlog")
    return _save(fig, path)


def outage_timeline(results: dict, outage: tuple[float, float], path: str) -> str:
    """Client-visible error rate and latency through an injected provider outage.

    Two panels because surviving an outage and surviving it cheaply are separate
    questions. The error-rate panel shows whether clients got answers; the
    latency panel shows what those answers cost, and makes visible that a
    breaker helps both configurations -- with failover it stops paying to
    rediscover the outage on every request, and without it, failures at least
    become fast instead of slow.
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 6.4), sharex=True)
    colours = {"single": "#b0b0b0", "failover": "#3572b0"}

    for ax in (ax1, ax2):
        ax.axvspan(outage[0], outage[1], color="#f2c9c9", alpha=0.5,
                   label="provider returning 5xx")
        ax.grid(alpha=0.3)

    for key, result in results.items():
        series = result["series"]
        ax1.plot(series["t"], [e * 100 for e in series["error_rate"]],
                 "-", color=colours[key], label=result["arm"], lw=1.8)
        ax2.plot(series["t"], series["p99_ms"], "-", color=colours[key],
                 label=result["arm"], lw=1.8)

    # Mark, per arm, when the gateway stopped sending traffic to the dead
    # provider. The two arms trip at different times, so marking only one would
    # attribute the wrong moment to the other.
    for key, result in results.items():
        opened = next((t for t, state in result["breaker"] if state == "open"), None)
        if opened is None:
            continue
        for ax in (ax1, ax2):
            ax.axvline(opened, color=colours[key], ls="--", lw=1.2, alpha=0.9)
        ax1.annotate(f"breaker open ({result['arm']})", (opened, 52), fontsize=7,
                     color=colours[key], rotation=90, va="center",
                     xytext=(3, 0), textcoords="offset points")

    ax1.set_ylabel("client-visible errors (%)")
    ax1.set_ylim(-5, 105)
    ax1.set_title("Provider outage: what the client experienced")
    ax1.legend(fontsize=8, loc="center left")

    ax2.set_ylabel("p99 latency (ms)")
    ax2.set_xlabel("time (s)")
    ax2.set_yscale("log")
    ax2.legend(fontsize=8, loc="upper left")

    return _save(fig, path)

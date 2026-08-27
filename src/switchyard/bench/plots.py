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

"""Summaries and persistence for benchmark runs.

Percentiles are computed from the full sample, never from pre-aggregated
buckets, and runs are stored row-per-request so that any later analysis can
recompute whatever it needs without re-running the experiment.
"""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from switchyard.bench.loadgen import LoadSpec, RequestRecord

# A request is "good" if it completed successfully within this wall-clock budget.
# Goodput rather than throughput is the headline number under overload: a system
# in congestion collapse can show high throughput while completing almost nothing
# a client is still waiting for.
DEFAULT_SLO_S = 10.0


def percentiles(values: Sequence[float], ps: Iterable[float] = (50, 95, 99)) -> dict[str, float]:
    if not values:
        return {f"p{int(p)}": float("nan") for p in ps}
    arr = np.asarray(values, dtype=float)
    return {f"p{int(p)}": float(np.percentile(arr, p)) for p in ps}


def summarize(records: Sequence[RequestRecord], slo_s: float = DEFAULT_SLO_S) -> dict[str, Any]:
    """Human-readable run summary.

    `scheduling_lag_p99` comes first because it decides whether the rest of the
    numbers mean anything: if the generator could not fire on schedule, measured
    latency understates real latency and the run is invalid.
    """
    total = len(records)
    ok = [r for r in records if r.ok]
    latencies = [r.latency for r in ok if r.latency is not None]
    ttfts = [r.ttft for r in ok if r.ttft is not None]
    lags = [r.scheduling_lag for r in records]
    good = [r for r in ok if r.latency is not None and r.latency <= slo_s]

    span = 0.0
    if records:
        starts = [r.intended_start for r in records]
        ends = [r.completed_at for r in records if r.completed_at is not None]
        span = (max(ends) - min(starts)) if ends else 0.0

    errors: dict[str, int] = {}
    for r in records:
        if not r.ok:
            errors[r.error or f"http_{r.status}"] = errors.get(r.error or f"http_{r.status}", 0) + 1

    lag = percentiles(lags)
    lat = percentiles(latencies)
    ttft = percentiles(ttfts)

    return {
        "requests": total,
        "completed_ok": len(ok),
        "error_rate": round(1 - len(ok) / total, 4) if total else 0.0,
        "errors": errors or "-",
        "scheduling_lag_p50_ms": round(lag["p50"] * 1000, 2),
        "scheduling_lag_p99_ms": round(lag["p99"] * 1000, 2),
        "generator_kept_up": lag["p99"] < 0.05,
        "throughput_rps": round(len(ok) / span, 2) if span else 0.0,
        "goodput_rps": round(len(good) / span, 2) if span else 0.0,
        "latency_p50_ms": round(lat["p50"] * 1000, 1),
        "latency_p95_ms": round(lat["p95"] * 1000, 1),
        "latency_p99_ms": round(lat["p99"] * 1000, 1),
        "ttft_p50_ms": round(ttft["p50"] * 1000, 1),
        "ttft_p95_ms": round(ttft["p95"] * 1000, 1),
        "ttft_p99_ms": round(ttft["p99"] * 1000, 1),
        "output_tokens_total": sum(r.output_tokens for r in ok),
    }


def write_parquet(
    records: Sequence[RequestRecord], path: str, label: str = "", spec: LoadSpec | None = None
) -> None:
    """Persist a run row-per-request.

    Run parameters are carried in every row rather than in a sidecar file so a
    results directory can be concatenated without losing which run a row is from.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = [r.to_row() for r in records]
    for row in rows:
        row["label"] = label
        if spec is not None:
            row["spec_rate"] = spec.rate
            row["spec_duration_s"] = spec.duration_s
            row["spec_model"] = spec.model
            row["spec_seed"] = spec.seed
            row["spec_max_tokens"] = spec.max_tokens

    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def read_parquet(path: str):
    import pyarrow.parquet as pq

    return pq.read_table(path)

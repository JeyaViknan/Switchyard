"""Reading Prometheus exposition text into rates a person can act on.

The gateway exposes counters and histograms -- monotonic totals since process
start. A console needs the opposite: what is happening *now*. That means
keeping the previous scrape and dividing the difference by the elapsed time,
which brings two problems worth being explicit about.

**Counters reset.** When the gateway restarts, every total goes back to zero.
Subtracting naively yields a large negative number, which as a rate would
render as a nonsense spike in the wrong direction. A sample lower than the one
before it means a reset, and the honest reading of the new value is "this much
since the restart" -- so the delta is the new value itself. It slightly
understates the first interval after a restart, which is the right direction
to be wrong in.

**Percentiles cannot be averaged.** A p95 over the last minute is not the mean
of per-scrape p95s. It has to be recomputed from the histogram's bucket counts
over the window, by differencing cumulative buckets and interpolating inside
whichever bucket holds the 95th observation. That estimate is only as precise
as the bucket edges, so the console labels it as approximate rather than
implying more resolution than exists.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from prometheus_client.parser import text_string_to_metric_families

# Label sets are identity here, so they have to be hashable and order-stable.
LabelKey = tuple[tuple[str, str], ...]


def labels_of(labels: dict[str, str]) -> LabelKey:
    return tuple(sorted(labels.items()))


@dataclass(frozen=True, slots=True)
class Histogram:
    """One histogram series: cumulative bucket counts, plus count and sum."""

    buckets: tuple[tuple[float, float], ...]   # (le, cumulative_count), ascending
    count: float
    sum: float


@dataclass(frozen=True, slots=True)
class Scrape:
    """One parse of /metrics, with the clock reading that goes with it."""

    at: float                                   # monotonic seconds
    counters: dict[str, dict[LabelKey, float]] = field(default_factory=dict)
    gauges: dict[str, dict[LabelKey, float]] = field(default_factory=dict)
    histograms: dict[str, dict[LabelKey, Histogram]] = field(default_factory=dict)

    def counter(self, name: str, **labels: str) -> float:
        return self.counters.get(name, {}).get(labels_of(labels), 0.0)

    def gauge(self, name: str, **labels: str) -> float | None:
        return self.gauges.get(name, {}).get(labels_of(labels))

    def counter_total(self, name: str, **match: str) -> float:
        """Sum every series of a counter whose labels include `match`."""
        wanted = match.items()
        return sum(
            value
            for key, value in self.counters.get(name, {}).items()
            if wanted <= dict(key).items()
        )

    def label_values(self, name: str, label: str) -> set[str]:
        """Which values of `label` this scrape has series for."""
        return {
            dict(key)[label]
            for key in self.counters.get(name, {})
            if label in dict(key)
        }


def parse(text: str, at: float) -> Scrape:
    """Parse exposition text. `at` is a monotonic reading, not wall clock."""
    scrape = Scrape(at=at)
    for family in text_string_to_metric_families(text):
        if family.type == "histogram":
            buckets: dict[LabelKey, list[tuple[float, float]]] = {}
            counts: dict[LabelKey, float] = {}
            sums: dict[LabelKey, float] = {}
            for sample in family.samples:
                labels = dict(sample.labels)
                le = labels.pop("le", None)
                key = labels_of(labels)
                if sample.name.endswith("_bucket") and le is not None:
                    buckets.setdefault(key, []).append((float(le), sample.value))
                elif sample.name.endswith("_count"):
                    counts[key] = sample.value
                elif sample.name.endswith("_sum"):
                    sums[key] = sample.value
            series = scrape.histograms.setdefault(family.name, {})
            for key, edges in buckets.items():
                series[key] = Histogram(
                    buckets=tuple(sorted(edges)),
                    count=counts.get(key, 0.0),
                    sum=sums.get(key, 0.0),
                )
        else:
            target = scrape.counters if family.type == "counter" else scrape.gauges
            for sample in family.samples:
                # Keyed by the *sample* name, not the family name. The parser
                # strips `_total` from a counter family, so the family is
                # `switchyard_requests` while the exposition, the dashboards
                # and anyone reading this code say `switchyard_requests_total`.
                # Callers should not have to remember that asymmetry.
                if sample.name.endswith("_created"):
                    continue
                target.setdefault(sample.name, {})[labels_of(dict(sample.labels))] = sample.value
    return scrape


def delta(previous: float, current: float) -> float:
    """Counter increase between two scrapes, surviving a restart.

    A current value below the previous one can only mean the process restarted,
    because counters do not decrease. The new value is then the whole of what
    has been counted since, so that is the delta.
    """
    return current if current < previous else current - previous


def rate(previous: Scrape | None, current: Scrape, name: str, **labels: str) -> float:
    """Per-second rate of one counter series between two scrapes."""
    if previous is None:
        return 0.0
    elapsed = current.at - previous.at
    if elapsed <= 0:
        return 0.0
    return delta(previous.counter(name, **labels), current.counter(name, **labels)) / elapsed


def rate_total(previous: Scrape | None, current: Scrape, name: str, **match: str) -> float:
    """Per-second rate summed over every series matching `match`."""
    if previous is None:
        return 0.0
    elapsed = current.at - previous.at
    if elapsed <= 0:
        return 0.0
    return delta(
        previous.counter_total(name, **match), current.counter_total(name, **match)
    ) / elapsed


def quantile_from_buckets(
    previous: Histogram | None, current: Histogram, q: float = 0.95
) -> float | None:
    """Approximate a quantile over the window between two histogram scrapes.

    Works on the *difference* in cumulative bucket counts, so the answer
    describes the window rather than all time. Returns None when nothing was
    observed in the window -- there is no 95th percentile of no requests, and
    reporting 0 would read as "very fast" instead of "no data".

    The value is interpolated linearly inside the bucket that contains the
    target observation, which is the standard approach and carries the standard
    caveat: resolution is bounded by the bucket edges, and anything landing in
    the +Inf bucket can only be reported as "at least the last finite edge".
    """
    if not current.buckets:
        return None

    prev_by_le = dict(previous.buckets) if previous else {}
    if previous and current.count < previous.count:
        prev_by_le = {}                       # restart: the window is all of it

    deltas: list[tuple[float, float]] = []
    for le, cumulative in current.buckets:
        increase = cumulative - prev_by_le.get(le, 0.0)
        deltas.append((le, max(0.0, increase)))

    total = deltas[-1][1] if deltas else 0.0
    if total <= 0:
        return None

    target = q * total
    lower_edge = 0.0
    previous_cumulative = 0.0
    for le, cumulative in deltas:
        if cumulative >= target:
            if math.isinf(le):
                # Everything above the last finite edge. Report that edge
                # rather than infinity: a dashboard cannot plot inf, and
                # "at least this" is the honest floor.
                return lower_edge or None
            span = cumulative - previous_cumulative
            if span <= 0:
                return le
            within = (target - previous_cumulative) / span
            return lower_edge + (le - lower_edge) * within
        lower_edge = le if not math.isinf(le) else lower_edge
        previous_cumulative = cumulative
    return lower_edge or None

"""Open-loop, SSE-aware load generator.

Why open-loop
-------------
A closed-loop generator (N workers, each sending the next request when its
previous one returns) cannot offer more load than the system can absorb: when
the system slows down, the generator slows down with it. Queueing therefore
never appears in the measurement, and reported latency stays flat right up to
the point where the system falls over. That artifact is coordinated omission.

This generator instead fixes an arrival *schedule* up front -- Poisson arrivals
at rate lambda -- and fires each request at its scheduled time whether or not
earlier requests have finished. Every latency is measured from the request's
*intended* start, so time spent waiting because the system was busy is counted.

Reproducibility
---------------
Request ids are derived from `(seed, index)`, not randomly. The synthetic fleet
keys every per-request draw on `(run_seed, request_id)`, so random ids would mean
two runs of the same spec produced different output lengths, different time to
first token, and different fault decisions -- making an A/B comparison between
two scheduling policies a comparison of two different workloads. Deterministic
ids are what closes that loop.

Self-check
----------
An open-loop generator can itself become the bottleneck, and when it does the
measurement understates real latency. Three signals are reported on every run:

- `scheduling_lag` -- how late each request actually fired. Judged as a *ratio*
  of typical latency rather than against a fixed millisecond threshold, since
  20ms of lag is negligible against a 2s request and severe against a 50ms one.
- connection saturation -- if concurrent requests reach the client's connection
  limit, further arrivals block on the pool and the generator has silently
  stopped being open-loop.
- file-descriptor headroom -- checked before the run starts, so fd exhaustion
  surfaces as a clear error instead of masquerading as failures in the system
  under test.

Measurement window
------------------
Requests whose scheduled arrival falls in the warmup period are issued normally
but excluded from reported statistics. Warmup exists because the first requests
of a run pay for TCP connection establishment and cold code paths, which is not
what the experiment is measuring. Every reported statistic covers exactly the
requests with `in_window=True`.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import resource
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field

import httpx
import numpy as np
import orjson

# Default ceiling on concurrent connections from the generator. High enough not
# to constrain the offered load in normal use, low enough to stay clear of a
# typical file-descriptor limit.
DEFAULT_MAX_CONNECTIONS = 1000

# Lag is judged against typical latency: a run is suspect when p99 scheduling lag
# exceeds this fraction of median request latency.
LAG_RATIO_THRESHOLD = 0.05


@dataclass(slots=True)
class RequestRecord:
    """One request's timeline. All times are `time.perf_counter()` seconds."""

    request_id: str
    tenant: str
    intended_start: float
    actual_start: float = 0.0
    first_token_at: float | None = None
    completed_at: float | None = None
    status: int = 0
    output_tokens: int = 0
    prompt_tokens: int = 0
    finish_reason: str | None = None
    error: str | None = None
    # Reported by the gateway, so queue wait can be attributed without having to
    # infer it by subtracting an assumed service time from end-to-end latency.
    queue_wait_s: float | None = None
    in_window: bool = True
    inter_token_s: list[float] = field(default_factory=list)

    # -- derived, all measured from intended_start ------------------------

    @property
    def scheduling_lag(self) -> float:
        return self.actual_start - self.intended_start

    @property
    def ttft(self) -> float | None:
        if self.first_token_at is None:
            return None
        return self.first_token_at - self.intended_start

    @property
    def latency(self) -> float | None:
        if self.completed_at is None:
            return None
        return self.completed_at - self.intended_start

    @property
    def ok(self) -> bool:
        return self.status == 200 and self.error is None

    def to_row(self) -> dict[str, object]:
        """Row for persistence.

        The full inter-token sample list is kept, not just its mean. Pooling
        per-request means would make an inter-token tail percentile
        uncomputable, and inter-token latency is a tail metric -- a stream whose
        mean gap is fine but whose p99 gap is 400ms reads as stuttering to a
        user, and a mean cannot show that.
        """
        row = asdict(self)
        row.update(
            scheduling_lag=self.scheduling_lag,
            ttft=self.ttft,
            latency=self.latency,
            ok=self.ok,
            mean_inter_token_s=(
                float(np.mean(self.inter_token_s)) if self.inter_token_s else None
            ),
        )
        return row


@dataclass(frozen=True, slots=True)
class LoadSpec:
    url: str
    rate: float                 # requests per second (Poisson mean)
    duration_s: float
    model: str = "fast"
    tenants: tuple[str, ...] = ("t1",)
    prompt: str = "Summarize the following in a short paragraph."
    max_tokens: int = 4096
    seed: int = 1
    request_timeout_s: float = 120.0
    warmup_s: float = 0.0
    max_connections: int = DEFAULT_MAX_CONNECTIONS
    api_key: str | None = None


async def _run_one(
    client: httpx.AsyncClient, spec: LoadSpec, record: RequestRecord
) -> RequestRecord:
    record.actual_start = time.perf_counter()
    body = {
        "model": spec.model,
        "messages": [{"role": "user", "content": spec.prompt}],
        "stream": True,
        "max_tokens": spec.max_tokens,
    }
    headers = {
        "x-switchyard-request-id": record.request_id,
        "x-switchyard-tenant": record.tenant,
    }
    if spec.api_key:
        headers["authorization"] = f"Bearer {spec.api_key}"
    last_token_at: float | None = None
    try:
        async with client.stream(
            "POST", spec.url, json=body, headers=headers, timeout=spec.request_timeout_s
        ) as response:
            record.status = response.status_code
            if (queued := response.headers.get("x-switchyard-queue-wait-ms")) is not None:
                record.queue_wait_s = float(queued) / 1000.0
            if response.status_code != 200:
                await response.aread()
                record.error = f"http_{response.status_code}"
                record.completed_at = time.perf_counter()
                return record

            saw_terminal = False
            async for line in response.aiter_lines():
                if not line.startswith("data: "):
                    continue
                payload = line[6:]
                now = time.perf_counter()
                if payload == "[DONE]":
                    saw_terminal = True
                    break

                frame = orjson.loads(payload)
                choice = (frame.get("choices") or [{}])[0]
                if choice.get("delta", {}).get("content"):
                    if record.first_token_at is None:
                        record.first_token_at = now
                    else:
                        record.inter_token_s.append(now - (last_token_at or now))
                    last_token_at = now
                    record.output_tokens += 1
                if choice.get("finish_reason"):
                    record.finish_reason = choice["finish_reason"]
                # A gateway error arrives as a well-formed stream that ends in
                # [DONE] like any other -- the terminal frame carries the
                # failure. Judging success by stream shape alone would count
                # every provider outage as a completed request.
                if frame.get("error") or choice.get("finish_reason") == "provider_error":
                    record.error = frame.get("error", {}).get("type", "provider_error")
                if usage := frame.get("usage"):
                    record.prompt_tokens = usage.get("prompt_tokens", 0)
                    # Provider-reported usage is authoritative over our count.
                    record.output_tokens = usage.get("completion_tokens", record.output_tokens)

            if not saw_terminal:
                # Stream ended without [DONE]: truncated, not complete. Recording
                # this distinctly is the point of invariant I8.
                record.error = "truncated"
                record.finish_reason = record.finish_reason or "truncated"
    except (httpx.TimeoutException, httpx.HTTPError, ConnectionError) as exc:
        record.error = type(exc).__name__
    finally:
        record.completed_at = time.perf_counter()
    return record


def request_id_for(seed: int, index: int) -> str:
    """Stable id for the n-th request of a run.

    Same seed and index always yield the same id, which is what makes the
    synthetic fleet reproduce a workload exactly. Vary `seed` to get an
    independent sample; keep it fixed to compare policies on identical work.
    """
    return f"lg-{seed}-{index}"


def arrival_schedule(rate: float, duration_s: float, seed: int) -> np.ndarray:
    """Poisson arrival offsets, in seconds from run start.

    Exponential inter-arrival gaps accumulated forward, so a late fire never
    shifts subsequent scheduled times -- the schedule is fixed before the run
    begins and does not adapt to how the system is coping.
    """
    rng = np.random.default_rng(seed)
    gaps = rng.exponential(1.0 / rate, size=int(rate * duration_s * 1.5) + 16)
    times = np.cumsum(gaps)
    return times[times < duration_s]


def check_fd_headroom(max_connections: int) -> None:
    """Fail before the run rather than during it.

    File-descriptor exhaustion surfaces as connection errors that look exactly
    like the system under test refusing connections. Checking up front means the
    benchmark cannot quietly report the client's limits as the server's.
    """
    soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    needed = max_connections + 64          # sockets, plus the process's own files
    if soft != resource.RLIM_INFINITY and needed > soft:
        raise RuntimeError(
            f"generator needs ~{needed} file descriptors for "
            f"max_connections={max_connections}, but the soft limit is {soft}. "
            f"Raise it (ulimit -n {needed}) or lower --max-connections."
        )


@dataclass(slots=True)
class RunOutcome:
    """Records plus the generator's own health signals for the run."""

    records: list[RequestRecord]
    peak_concurrency: int
    connection_limited: bool


async def run_load(spec: LoadSpec) -> list[RequestRecord]:
    """Run a load spec and return its records. See `run_load_detailed`."""
    return (await run_load_detailed(spec)).records


async def run_load_detailed(spec: LoadSpec) -> RunOutcome:
    check_fd_headroom(spec.max_connections)

    offsets = arrival_schedule(spec.rate, spec.duration_s, spec.seed)
    records: list[RequestRecord] = []
    tasks: list[asyncio.Task[RequestRecord]] = []

    live = 0
    peak = 0

    limits = httpx.Limits(
        max_connections=spec.max_connections,
        max_keepalive_connections=spec.max_connections,
    )
    async with httpx.AsyncClient(limits=limits) as client:
        run_start = time.perf_counter()

        async def tracked(record: RequestRecord) -> RequestRecord:
            nonlocal live, peak
            live += 1
            peak = max(peak, live)
            try:
                return await _run_one(client, spec, record)
            finally:
                live -= 1

        for i, offset in enumerate(offsets):
            intended = run_start + float(offset)
            delay = intended - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)

            record = RequestRecord(
                # Deterministic, not random: see "Reproducibility" above.
                request_id=request_id_for(spec.seed, i),
                tenant=spec.tenants[i % len(spec.tenants)],
                intended_start=intended,
                in_window=float(offset) >= spec.warmup_s,
            )
            records.append(record)
            tasks.append(asyncio.create_task(tracked(record)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    return RunOutcome(
        records=records,
        peak_concurrency=peak,
        # At the cap, further arrivals block on the connection pool instead of
        # being issued -- the generator has stopped being open-loop.
        connection_limited=peak >= spec.max_connections,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Open-loop SSE load generator")
    parser.add_argument("--url", required=True)
    parser.add_argument("--rate", type=float, required=True, help="requests/sec (Poisson)")
    parser.add_argument("--duration", type=float, required=True, help="seconds")
    parser.add_argument("--model", default="fast")
    parser.add_argument("--tenants", default="t1", help="comma-separated")
    parser.add_argument("--max-tokens", type=int, default=4096)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--out", default=None, help="parquet path")
    parser.add_argument("--label", default="", help="run label stored in the output")
    args = parser.parse_args(argv)

    spec = LoadSpec(
        url=args.url, rate=args.rate, duration_s=args.duration, model=args.model,
        tenants=tuple(args.tenants.split(",")), max_tokens=args.max_tokens, seed=args.seed,
    )
    records = asyncio.run(run_load(spec))

    from switchyard.bench.stats import summarize, write_parquet

    summary = summarize(records)
    for key, value in summary.items():
        print(f"{key:24} {value}")

    if args.out:
        write_parquet(records, args.out, label=args.label, spec=spec)
        print(f"{'wrote':24} {args.out}")
    return 0


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        raise SystemExit(main())

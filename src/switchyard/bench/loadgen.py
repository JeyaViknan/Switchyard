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

Self-check
----------
An open-loop generator can itself become the bottleneck. `scheduling_lag`
(actual start minus intended start) measures that directly. If it grows over a
run, the generator could not keep up and the run's latency numbers understate
the truth -- the run should be discarded or re-run with less load per process.
Reporting it is not optional; it is what makes the rest of the numbers credible.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field

import httpx
import numpy as np
import orjson


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
        row = asdict(self)
        row.pop("inter_token_s")
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
    last_token_at: float | None = None
    try:
        async with client.stream(
            "POST", spec.url, json=body, headers=headers, timeout=spec.request_timeout_s
        ) as response:
            record.status = response.status_code
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


async def run_load(spec: LoadSpec) -> list[RequestRecord]:
    offsets = arrival_schedule(spec.rate, spec.duration_s, spec.seed)
    records: list[RequestRecord] = []
    tasks: list[asyncio.Task[RequestRecord]] = []

    limits = httpx.Limits(max_connections=None, max_keepalive_connections=None)
    async with httpx.AsyncClient(limits=limits) as client:
        run_start = time.perf_counter()
        for i, offset in enumerate(offsets):
            intended = run_start + float(offset)
            delay = intended - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)

            record = RequestRecord(
                request_id=f"lg-{uuid.uuid4().hex[:12]}",
                tenant=spec.tenants[i % len(spec.tenants)],
                intended_start=intended,
            )
            records.append(record)
            tasks.append(asyncio.create_task(_run_one(client, spec, record)))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    return records


def _cancel_all(tasks: Sequence[asyncio.Task[RequestRecord]]) -> None:
    for task in tasks:
        if not task.done():
            task.cancel()


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

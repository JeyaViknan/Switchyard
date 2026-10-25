"""Demo traffic, generated open-loop and capped so it cannot hurt the host.

Open-loop means arrivals are scheduled independently of whether earlier ones
have finished -- exponential gaps, Poisson arrivals -- which is the same
discipline `bench/loadgen.py` follows and for the same reason. A closed-loop
generator (fire, wait, fire again) cannot offer more load than the system can
absorb, so it can never show a tenant being turned away. The noisy-neighbour
story depends on offering more than there is room for.

Every cap here exists because this runs on a free-tier t3.micro with 1 GB of
RAM beside the gateway, the fleet, Prometheus and Grafana. A demo that makes
the box unresponsive during an interview is worse than no demo, so the limits
are deliberately lower than what the gateway could take.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from switchyard.obs.logs import event, get_logger

log = get_logger("console.traffic")

# --- caps, all because of the t3.micro -----------------------------------
# Total arrival rate across every tenant. The gateway's own capacity is 24
# concurrent requests; offering ~30/s is enough to overflow it and show
# rejection, without the generator itself becoming the bottleneck.
MAX_TOTAL_RPS = 30.0
# Open streams held at once. Each is a socket on both sides plus a task.
MAX_OPEN_STREAMS = 120
# Nothing runs forever. A profile left running after a demo would keep the
# box warm, burn budget and make every later screenshot misleading.
MAX_PROFILE_DURATION_S = 120.0

REQUEST_TIMEOUT_S = 30.0
OUTPUT_TOKENS = (200, 400)


@dataclass(frozen=True, slots=True)
class Phase:
    """Per-tenant arrival rates held for a span of seconds."""

    duration_s: float
    rates: dict[str, float]


@dataclass(frozen=True, slots=True)
class Profile:
    name: str
    headline: str
    watch_for: str
    phases: tuple[Phase, ...]

    @property
    def duration_s(self) -> float:
        return sum(p.duration_s for p in self.phases)

    def validate(self) -> None:
        for phase in self.phases:
            total = sum(phase.rates.values())
            if total > MAX_TOTAL_RPS:
                raise ValueError(
                    f"profile {self.name!r} offers {total} rps in one phase, "
                    f"above the {MAX_TOTAL_RPS} rps cap this host is sized for"
                )
        if self.duration_s > MAX_PROFILE_DURATION_S:
            raise ValueError(
                f"profile {self.name!r} runs {self.duration_s}s, above the "
                f"{MAX_PROFILE_DURATION_S}s cap"
            )


STEADY = Profile(
    name="steady",
    headline="Normal traffic from three tenants.",
    watch_for="Everyone is served; queues stay near zero.",
    # Long enough to outlast the provider-outage scenario. A half-open
    # breaker closes by succeeding on a probe, and a probe needs a request:
    # if traffic stops first, the breaker can never close and the demo ends
    # on a failure that is an artefact of the generator, not the gateway.
    phases=(Phase(100.0, {"acme": 1.5, "globex": 1.0, "initech": 0.3}),),
)

NOISY_NEIGHBOUR = Profile(
    name="noisy-neighbour",
    headline="One tenant floods while another keeps working normally.",
    watch_for=(
        "acme's lane fills and its requests bounce off the gateway in red, "
        "while globex keeps being served from its reserved floor."
    ),
    phases=(
        Phase(10.0, {"acme": 1.5, "globex": 1.0}),      # establish normal
        Phase(30.0, {"acme": 25.0, "globex": 1.0}),     # the flood
        Phase(20.0, {"acme": 1.5, "globex": 1.0}),      # recovery
    ),
)

PROFILES: dict[str, Profile] = {p.name: p for p in (STEADY, NOISY_NEIGHBOUR)}
for _profile in PROFILES.values():
    _profile.validate()


@dataclass(slots=True)
class TrafficGenerator:
    """Runs one profile at a time against the gateway."""

    gateway_url: str
    keys: dict[str, str]
    client: httpx.AsyncClient
    model: str = "fast"
    seed: int = 7

    profile: Profile | None = None
    started_at: float = 0.0
    _task: asyncio.Task[None] | None = None
    _open: int = 0
    _sent: int = 0
    _dropped_for_cap: int = 0
    _streams: set[asyncio.Task[None]] = field(default_factory=set)

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def state(self) -> dict[str, Any]:
        if not self.running or self.profile is None:
            return {"profile": None, "running": False}
        elapsed = time.monotonic() - self.started_at
        return {
            "profile": self.profile.name,
            "running": True,
            "ends_in_s": max(0.0, round(self.profile.duration_s - elapsed, 1)),
            "open_streams": self._open,
            "sent": self._sent,
            "dropped_for_cap": self._dropped_for_cap,
        }

    async def start(self, name: str) -> None:
        """Start a profile, replacing any that is running.

        One at a time: two overlapping profiles would make the map
        uninterpretable and the verdicts meaningless.
        """
        if name not in PROFILES:
            raise KeyError(name)
        if not self.keys:
            raise RuntimeError("no demo tenant keys configured")
        await self.stop()
        self.profile = PROFILES[name]
        self.started_at = time.monotonic()
        self._sent = self._dropped_for_cap = 0
        self._task = asyncio.create_task(self._run(self.profile))
        event(log, "console.traffic_started", profile=name,
              duration_s=self.profile.duration_s)

    async def stop(self) -> None:
        """Stop generating and cancel in-flight streams cleanly."""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        for stream in list(self._streams):
            stream.cancel()
        if self._streams:
            await asyncio.gather(*self._streams, return_exceptions=True)
            self._streams.clear()
        self._open = 0
        if self.profile is not None:
            event(log, "console.traffic_stopped", profile=self.profile.name)
        self.profile = None

    async def _run(self, profile: Profile) -> None:
        rng = random.Random(self.seed)
        try:
            for phase in profile.phases:
                await self._run_phase(phase, rng)
        finally:
            # Auto-stop is a guarantee, not a hope: whether the profile ran out
            # of phases, raised, or was cancelled, the streams get cancelled.
            for stream in list(self._streams):
                stream.cancel()

    async def _run_phase(self, phase: Phase, rng: random.Random) -> None:
        deadline = time.monotonic() + phase.duration_s
        tenants = [(t, r) for t, r in phase.rates.items() if r > 0 and t in self.keys]
        if not tenants:
            await asyncio.sleep(phase.duration_s)
            return
        await asyncio.gather(*(self._arrivals(t, r, deadline, rng) for t, r in tenants))

    async def _arrivals(
        self, tenant: str, rate: float, deadline: float, rng: random.Random
    ) -> None:
        """Poisson arrivals: exponential gaps, fired regardless of completion."""
        while time.monotonic() < deadline:
            await asyncio.sleep(rng.expovariate(rate))
            if time.monotonic() >= deadline:
                return
            if self._open >= MAX_OPEN_STREAMS:
                # Drop rather than queue. Queueing here would quietly turn the
                # generator closed-loop, which is the measurement error this
                # whole design exists to avoid -- and it is counted, so the
                # console can say the generator was the limit, not the gateway.
                self._dropped_for_cap += 1
                continue
            task = asyncio.create_task(self._one(tenant, rng))
            self._streams.add(task)
            task.add_done_callback(self._streams.discard)

    async def _one(self, tenant: str, rng: random.Random) -> None:
        self._open += 1
        self._sent += 1
        try:
            body = {
                "model": self.model,
                "messages": [{"role": "user", "content": "demo traffic"}],
                "max_tokens": rng.randint(*OUTPUT_TOKENS),
                "stream": True,
            }
            async with self.client.stream(
                "POST", f"{self.gateway_url}/v1/chat/completions", json=body,
                headers={"authorization": f"Bearer {self.keys[tenant]}"},
                timeout=REQUEST_TIMEOUT_S,
            ) as response:
                # Drained and discarded: the console cares that the request
                # happened and how the gateway accounted for it, never about
                # the content. Nothing generated here is shown to anyone.
                async for _ in response.aiter_bytes():
                    pass
        except (TimeoutError, httpx.HTTPError):
            # A rejected or failed request is a normal outcome of a demo that
            # deliberately overloads the gateway, and the gateway's own metrics
            # already record it. Nothing to do here but let go of the slot.
            pass
        finally:
            self._open -= 1

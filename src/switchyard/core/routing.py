"""Provider selection and failover.

The failover policy, and why it is phased
-----------------------------------------
What can be done about a provider failure depends entirely on whether anything
has already reached the client.

*Before the first token* nothing has been delivered, so another provider can be
tried and the client never learns that the first one failed. This covers the
majority of provider failures -- connection errors, 5xx, 429, and a provider
that accepts a request and then never answers.

*After the first token* it cannot. Switching providers mid-response would append
a second, different continuation to a partial one and hand the client a corrupted
answer with a success status. That is strictly worse than an honest failure: a
visible error can be retried by the caller, while silently corrupt output cannot
even be detected. So a mid-stream failure terminates with a typed error frame
carrying how many tokens actually arrived.

This is why the TTFT deadline is deliberately tight. It is the lever that moves
failure mass out of the unrecoverable window and into the one where recovery is
invisible.

What failover does not do
-------------------------
It does not acquire a second capacity lease or a second budget reservation. The
request holds exactly one slot for its whole life, retries included, because it
is still one request competing for capacity. And because failover only happens
when zero tokens were produced, no output has been generated to double-charge.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass

from switchyard.core.health import HealthRegistry
from switchyard.types import (
    CompletionRequest,
    ErrorClass,
    ProviderAdapter,
    StreamDone,
    StreamEvent,
    StreamFailed,
    TokenChunk,
)


@dataclass(frozen=True, slots=True)
class Attempt:
    """One provider tried, and what happened."""

    provider: str
    outcome: str                       # "ok", "skipped_breaker_open", or an error class
    tokens: int = 0


class RouteObserver:
    """Hooks the gateway uses to record what routing did. Defaults do nothing."""

    def on_failover(self, request_id: str, frm: str, to: str, reason: ErrorClass) -> None: ...
    def on_skipped(self, request_id: str, provider: str) -> None: ...
    def on_terminal_failure(
        self, provider: str, error_class: ErrorClass, mid_stream: bool
    ) -> None: ...
    def on_success(self, provider: str, attempts: int) -> None: ...


class ProviderRouter:
    """Runs a request against an ordered list of providers, failing over safely."""

    def __init__(
        self,
        adapters: Mapping[str, ProviderAdapter],
        routes: Mapping[str, Sequence[str]],
        health: HealthRegistry,
        observer: RouteObserver | None = None,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._adapters = dict(adapters)
        self._routes = {model: tuple(c) for model, c in routes.items()}
        self._health = health
        self._observer = observer or RouteObserver()
        self._clock = clock

    def candidates(self, model: str) -> tuple[str, ...]:
        """Providers to try for a model, in order.

        A model with no explicit route maps to the provider of the same name,
        which keeps single-provider setups free of configuration.
        """
        route = self._routes.get(model)
        if route:
            return tuple(name for name in route if name in self._adapters)
        return (model,) if model in self._adapters else ()

    def knows(self, model: str) -> bool:
        return bool(self.candidates(model))

    async def stream(
        self, request: CompletionRequest, observer: RouteObserver | None = None
    ) -> AsyncIterator[StreamEvent]:
        """Run `request` against its candidates.

        `observer` is accepted per call because routing is a per-request
        decision: which provider served it, and how many attempts it took, are
        facts about this request rather than about the router.
        """
        obs = observer or self._observer
        candidates = self.candidates(request.model)
        attempts: list[Attempt] = []
        last_error: ErrorClass | None = None
        last_provider: str | None = None

        for index, name in enumerate(candidates):
            health = self._health.get(name)
            if not health.allow():
                attempts.append(Attempt(name, "skipped_breaker_open"))
                obs.on_skipped(request.request_id, name)
                continue

            emitted = 0
            ttft_s: float | None = None
            started = self._clock()
            verdict_recorded = False

            try:
                async for event in self._adapters[name].stream(request):
                    if isinstance(event, TokenChunk):
                        if emitted == 0:
                            ttft_s = self._clock() - started
                        emitted += 1
                        yield event

                    elif isinstance(event, StreamDone):
                        health.record_success(ttft_s)
                        verdict_recorded = True
                        attempts.append(Attempt(name, "ok", emitted))
                        obs.on_success(name, len(attempts))
                        yield event
                        return

                    else:                                   # StreamFailed
                        health.record_failure(event.error_class)
                        verdict_recorded = True
                        last_error = event.error_class
                        last_provider = name
                        attempts.append(Attempt(name, event.error_class.value, emitted))

                        if emitted > 0 or not event.error_class.retryable:
                            # Nothing to fall back to: either the client already
                            # holds part of this answer, or the request would
                            # fail the same way anywhere.
                            obs.on_terminal_failure(
                                name, event.error_class, mid_stream=emitted > 0
                            )
                            yield event
                            return

                        # Nothing delivered and worth another try. Move on; if
                        # this was the last candidate the loop falls through to
                        # the exhaustion path, which reports every attempt
                        # rather than only whichever one happened to be last.
                        if index + 1 < len(candidates):
                            obs.on_failover(
                                request.request_id, name, candidates[index + 1],
                                event.error_class,
                            )
                        break
            finally:
                # The consumer may have gone away mid-stream, in which case no
                # verdict was reached. The probe slot still has to be returned,
                # or the breaker can never gather enough evidence to close.
                if not verdict_recorded:
                    health.record_abandoned()

        # Every candidate was unavailable or failed before producing anything.
        # Nothing reached the client, so this is still a clean failure.
        error_class = last_error or ErrorClass.CONNECT
        if last_provider is not None:
            obs.on_terminal_failure(last_provider, error_class, mid_stream=False)
        yield StreamFailed(
            error_class=error_class,
            message=_exhausted_message(request.model, candidates, attempts),
            chunks_emitted=0,
        )

    def describe(self, model: str) -> dict[str, object]:
        return {
            "candidates": list(self.candidates(model)),
            "health": {
                name: self._health.get(name).state.value for name in self.candidates(model)
            },
        }


def _exhausted_message(
    model: str, candidates: Sequence[str], attempts: Sequence[Attempt]
) -> str:
    """Name every provider tried and how each failed.

    Reporting only the last error hides that a failover happened at all, which
    is exactly the information needed to tell one bad provider from an outage
    affecting all of them.
    """
    if not candidates:
        return f"no providers are configured for model {model!r}"
    detail = ", ".join(f"{a.provider}: {a.outcome}" for a in attempts) or "none attempted"
    if len(attempts) == 1:
        return f"provider for model {model!r} is unavailable ({detail})"
    return f"all providers for model {model!r} are unavailable ({detail})"

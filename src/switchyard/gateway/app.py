"""The Switchyard gateway.

A request's path: authenticate, validate, estimate what it will cost, acquire a
capacity slot (queueing if the gateway is busy, rejected if it cannot be served
in time), stream it, then settle the estimate against what it actually used.

Admission before the response starts
------------------------------------
For streaming requests the capacity slot is acquired *before* the
`StreamingResponse` is constructed, not inside its body. A rejection must be an
HTTP status the client can act on, and once the response body has started the
status line is already sent -- a 200 followed by an error frame is a much worse
way to say "we are full" than a 429.

Unsupported request fields are rejected by name rather than ignored. A client
that asked for tool calls and got a plain completion has been given a wrong
answer, not a degraded one.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from switchyard.adapters.synthetic import SyntheticAdapter, build_client
from switchyard.core.auth import AuthError, TenantRegistry, bearer_from_header
from switchyard.core.budget import BudgetExceeded, BudgetLedger
from switchyard.core.config import DEFAULT_CONFIG_PATH, GatewayConfig, Tenant, load_config
from switchyard.core.health import BreakerState, HealthRegistry
from switchyard.core.prediction import OutputLengthPredictor
from switchyard.core.routing import ProviderRouter, RouteObserver
from switchyard.core.scheduler import AdmissionRejected, RejectReason, Scheduler
from switchyard.gateway.stream import collect, to_sse
from switchyard.obs.metrics import (
    ADMISSION_REJECTED,
    BREAKER_STATE,
    BUDGET_REMAINING,
    BUDGET_RESERVED,
    BUDGET_SPENT,
    CAPACITY_UTILISATION,
    DISPATCHED,
    FAILOVERS,
    MAX_TOKENS_CLAMPED,
    PREDICTION_ERROR,
    PROVIDER_ERRORS,
    PROVIDER_SKIPPED,
    QUEUE_DEPTH,
    REGISTRY,
    TENANT_INFLIGHT,
    TENANT_TOKENS,
    TERMINAL_FAILURES,
    RequestTimeline,
    monitor_event_loop_lag,
)
from switchyard.types import CompletionRequest, Message

UNSUPPORTED_FIELDS = (
    "tools", "functions", "tool_choice", "function_call", "logprobs",
    "top_logprobs", "response_format", "seed", "logit_bias", "stop",
)

MAX_MESSAGES = 64
MAX_PROMPT_CHARS = 100_000
DEFAULT_MAX_TOKENS = 512

# Used when the configuration declares no tenants at all, so that a fresh
# checkout serves requests without a setup step. Any configuration with tenants
# requires authentication; /health reports which mode is active.
OPEN_TENANT = Tenant(id="default", key_sha256="0" * 64, max_queue_depth=1024)

_REJECT_STATUS = {
    RejectReason.QUEUE_FULL: 429,
    RejectReason.DEADLINE: 503,
    RejectReason.SHUTTING_DOWN: 503,
}

# 402 rather than 429: a budget is exhausted, not busy. Retrying will not help,
# and telling a client to retry something that can never succeed is worse than
# telling it nothing.
BUDGET_STATUS = 402


def parse_request(body: dict[str, Any], request_id: str, max_tokens_cap: int) -> CompletionRequest:
    if not isinstance(body, dict):
        raise HTTPException(400, "request body must be a JSON object")

    for field in UNSUPPORTED_FIELDS:
        if body.get(field) is not None:
            raise HTTPException(400, f"unsupported field {field!r}")
    if body.get("n", 1) != 1:
        raise HTTPException(400, "unsupported field 'n': only n=1 is supported")

    model = body.get("model")
    if not isinstance(model, str) or not model:
        raise HTTPException(400, "'model' is required")

    raw_messages = body.get("messages")
    if not isinstance(raw_messages, list) or not raw_messages:
        raise HTTPException(400, "'messages' must be a non-empty array")
    if len(raw_messages) > MAX_MESSAGES:
        raise HTTPException(400, f"too many messages (max {MAX_MESSAGES})")

    messages = []
    total_chars = 0
    for m in raw_messages:
        role, content = m.get("role"), m.get("content")
        if role not in ("system", "user", "assistant"):
            raise HTTPException(400, f"unsupported message role {role!r}")
        if not isinstance(content, str):
            raise HTTPException(400, "message content must be a string")
        total_chars += len(content)
        messages.append(Message(role=role, content=content))

    # Oversized prompts are refused before any capacity is consumed. One very
    # large prompt costs far more than an average one, so admitting it as a
    # single unit of work would be a resource-asymmetry hole.
    if total_chars > MAX_PROMPT_CHARS:
        raise HTTPException(400, f"prompt too large ({total_chars} > {MAX_PROMPT_CHARS} chars)")

    # `or` would treat an explicit 0 as absent and silently substitute the
    # default, which is the silent-ignore behaviour this validation prevents.
    raw_max_tokens = body.get("max_tokens")
    max_tokens = DEFAULT_MAX_TOKENS if raw_max_tokens is None else int(raw_max_tokens)
    if max_tokens < 1:
        raise HTTPException(400, "'max_tokens' must be >= 1")
    max_tokens = min(max_tokens, max_tokens_cap)

    temperature = float(body.get("temperature", 0.0))
    if not 0.0 <= temperature <= 2.0:
        raise HTTPException(400, "'temperature' must be in [0, 2]")

    return CompletionRequest(
        model=model, messages=tuple(messages), max_tokens=max_tokens,
        temperature=temperature, stream=bool(body.get("stream", False)),
        request_id=request_id,
    )


_BREAKER_CODE = {BreakerState.CLOSED: 0, BreakerState.HALF_OPEN: 1, BreakerState.OPEN: 2}


class MetricsRouteObserver(RouteObserver):
    """Records one request's routing decisions.

    Built per request so it can also capture which provider actually served the
    response, which is not known until one answers and may not be the first one
    tried.
    """

    __slots__ = ("timeline", "attempts", "failed_over")

    def __init__(self, timeline) -> None:
        self.timeline = timeline
        self.attempts = 0
        self.failed_over = False

    def on_failover(self, request_id, frm, to, reason) -> None:
        self.failed_over = True
        FAILOVERS.labels(from_provider=frm, to_provider=to).inc()
        PROVIDER_ERRORS.labels(provider=frm, error_class=reason.value).inc()

    def on_skipped(self, request_id, provider) -> None:
        PROVIDER_SKIPPED.labels(provider=provider).inc()

    def on_terminal_failure(self, provider, error_class, mid_stream) -> None:
        self.timeline.provider = provider
        PROVIDER_ERRORS.labels(provider=provider, error_class=error_class.value).inc()
        TERMINAL_FAILURES.labels(
            provider=provider, error_class=error_class.value,
            phase="mid_stream" if mid_stream else "pre_first_token",
        ).inc()

    def on_success(self, provider, attempts) -> None:
        self.timeline.provider = provider
        self.attempts = attempts


def _budget_error(tenant_id: str, remaining: int, request_id: str) -> JSONResponse:
    return JSONResponse(
        {"error": {"type": "budget_exhausted", "tenant": tenant_id,
                   "remaining_tokens": remaining, "request_id": request_id}},
        status_code=BUDGET_STATUS,
    )


def _routing_headers(observer) -> dict[str, str]:
    """Non-streaming only: which provider served this, and whether it failed over.

    Streaming responses cannot carry it -- headers are sent before a provider has
    answered, which is precisely the point at which failover can still happen.
    """
    headers = {}
    if observer.timeline.provider != "-":
        headers["x-switchyard-provider"] = observer.timeline.provider
    if observer.failed_over:
        headers["x-switchyard-failed-over"] = "true"
    return headers


def _response_headers(lease, reservation) -> dict[str, str]:
    """Tell the client what the gateway decided on its behalf.

    Queue wait explains a slow response that was not the model's fault, and a
    clamped `max_tokens` explains a short one. Both are things a client would
    otherwise have to guess at.
    """
    headers = {"x-switchyard-queue-wait-ms": f"{lease.queue_wait_s * 1000:.1f}"}
    if reservation.clamped:
        headers["x-switchyard-max-tokens-clamped"] = str(reservation.effective_max_tokens)
    return headers


def _budget_view(tenant, ledger, predictor, model: str) -> dict[str, Any]:
    """A tenant's budget position, including how much longer it can keep going.

    `requests_remaining` divides available tokens by the *typical* request size
    rather than the ceiling, because that is the question an operator actually
    has: not how many requests could theoretically fit, but roughly how many
    more this tenant will get before it runs out.
    """
    snapshot = ledger.snapshot().get(tenant.id, {})
    if snapshot.get("limit") is None:
        return {"limit": None}
    available = snapshot.get("available") or 0
    typical = max(predictor.estimate(tenant.id, model, tenant.max_tokens_cap).p50, 1.0)
    return {
        "limit": snapshot["limit"],
        "spent": snapshot["spent"],
        "reserved_in_flight": snapshot["reserved"],
        "available": available,
        "requests_remaining_estimate": int(available // typical),
    }


def create_app(
    config: GatewayConfig | None = None,
    fleet_url: str | None = None,
    providers: tuple[str, ...] | None = None,
) -> FastAPI:
    if config is None:
        config = load_config(os.environ.get("SWITCHYARD_CONFIG", DEFAULT_CONFIG_PATH))
    fleet_url = fleet_url or os.environ.get("SWITCHYARD_FLEET_URL") or config.fleet_url
    providers = providers or config.providers

    registry = TenantRegistry.from_config(config)
    auth_required = bool(config.tenants)
    if not auth_required:
        # Open mode: the scheduler still needs a tenant to account against, but
        # the default tenant is deliberately absent from the auth registry so it
        # cannot be authenticated as.
        config = replace(config, tenants=(OPEN_TENANT,))
    predictor = OutputLengthPredictor()

    def on_dispatch(lease) -> None:
        DISPATCHED.labels(tenant=lease.tenant_id).inc()

    def on_reject(tenant_id: str, reason: RejectReason) -> None:
        ADMISSION_REJECTED.labels(tenant=tenant_id, reason=reason.value).inc()

    scheduler = Scheduler(config, on_dispatch=on_dispatch, on_reject=on_reject)
    ledger = BudgetLedger.from_tenants(config.tenants)
    provider_health = HealthRegistry(providers, config.breaker)

    def publish_scheduler_gauges() -> None:
        stats = scheduler.stats()
        CAPACITY_UTILISATION.set(stats.inflight / config.max_concurrency)
        for tenant_id, depth in stats.per_tenant_queue_depth.items():
            QUEUE_DEPTH.labels(tenant=tenant_id).set(depth)
            TENANT_INFLIGHT.labels(tenant=tenant_id).set(
                stats.per_tenant_inflight.get(tenant_id, 0)
            )
        for name, snapshot in provider_health.snapshot().items():
            BREAKER_STATE.labels(provider=name).set(
                _BREAKER_CODE[BreakerState(snapshot["state"])]
            )
        for tenant_id, budget in ledger.snapshot().items():
            if budget["limit"] is not None:
                BUDGET_REMAINING.labels(tenant=tenant_id).set(budget["available"] or 0)
            BUDGET_RESERVED.labels(tenant=tenant_id).set(budget["reserved"] or 0)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = build_client(
            max_connections=max(1000, config.max_concurrency * 4), timeouts=config.timeouts
        )
        app.state.client = client
        app.state.config = config
        app.state.scheduler = scheduler
        app.state.predictor = predictor
        app.state.ledger = ledger
        adapters = {
            name: SyntheticAdapter(name, fleet_url, client, config.timeouts)
            for name in providers
        }
        app.state.adapters = adapters
        app.state.router = ProviderRouter(adapters, config.routes, provider_health)
        app.state.provider_health = provider_health
        lag_task = asyncio.create_task(monitor_event_loop_lag())
        try:
            yield
        finally:
            lag_task.cancel()
            # Refuse queued work immediately, let running streams finish.
            result = await scheduler.drain(config.drain_timeout_s)
            app.state.drain_result = result
            await client.aclose()

    app = FastAPI(title="Switchyard", lifespan=lifespan)

    def resolve_tenant(request: Request) -> Tenant:
        if not auth_required:
            return OPEN_TENANT
        try:
            return registry.authenticate(bearer_from_header(request.headers.get("authorization")))
        except AuthError as exc:
            raise HTTPException(401, str(exc)) from None

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request) -> Response:
        timeline = RequestTimeline()
        tenant = resolve_tenant(request)
        timeline.tenant_id = tenant.id

        request_id = request.headers.get("x-switchyard-request-id") or f"sy-{uuid.uuid4().hex[:12]}"
        parsed = parse_request(await request.json(), request_id, tenant.max_tokens_cap)

        router: ProviderRouter = request.app.state.router
        if not router.knows(parsed.model):
            raise HTTPException(404, f"unknown model {parsed.model!r}")

        # Cheap, non-binding budget check before the request is allowed to
        # queue. A request that cannot be paid for should not occupy a queue
        # slot that another tenant's request could have used.
        if not ledger.has_headroom(tenant.id):
            return _budget_error(tenant.id, ledger.remaining(tenant.id), request_id)

        # p50, not the ceiling: this is the scheduler's unbiased guess at what
        # the request will consume, used only to order the queue, and corrected
        # against the real figure when the lease settles.
        estimate = predictor.estimate(tenant.id, parsed.model, parsed.max_tokens)

        stack = AsyncExitStack()
        try:
            lease = await stack.enter_async_context(scheduler.acquire(tenant, estimate.p50))
            # Binding reservation, taken after the request has capacity rather
            # than before it queues: a reservation held through a long queue
            # wait would reject other requests the tenant could actually afford.
            reservation = stack.enter_context(ledger.reserve(tenant.id, parsed.max_tokens))
        except AdmissionRejected as exc:
            await stack.aclose()
            publish_scheduler_gauges()
            return JSONResponse(
                {"error": {"type": exc.reason.value, "message": exc.message,
                           "request_id": request_id}},
                status_code=_REJECT_STATUS[exc.reason],
                headers={"retry-after": "1"} if exc.reason is RejectReason.QUEUE_FULL else None,
            )
        except BudgetExceeded as exc:
            await stack.aclose()
            publish_scheduler_gauges()
            return _budget_error(tenant.id, exc.remaining, request_id)

        # The reservation is held at the request's ceiling, so the ceiling has to
        # be what the provider is actually told. Without this the request could
        # emit more than was reserved and the spending bound would be advisory.
        if reservation.clamped:
            parsed = replace(parsed, max_tokens=reservation.effective_max_tokens)
            MAX_TOKENS_CLAMPED.labels(tenant=tenant.id).inc()

        timeline.record_queue_wait(lease.queue_wait_s)
        publish_scheduler_gauges()

        def finish(tokens: int) -> None:
            """Record what the request really used, everywhere it matters."""
            lease.actual_tokens = tokens          # settles the fairness clock
            reservation.actual = tokens           # settles the budget
            if tokens > 0:
                predictor.observe(tenant.id, parsed.model, tokens)
                TENANT_TOKENS.labels(tenant=tenant.id).inc(tokens)
                BUDGET_SPENT.labels(tenant=tenant.id).inc(tokens)
                PREDICTION_ERROR.observe(tokens / max(estimate.p50, 1.0))
            publish_scheduler_gauges()

        headers = _response_headers(lease, reservation)
        observer = MetricsRouteObserver(timeline)

        if not parsed.stream:
            async with stack:
                payload = await collect(router.stream(parsed, observer), parsed, timeline)
                finish(int(payload["usage"]["completion_tokens"]))
            return JSONResponse(payload, headers=headers | _routing_headers(observer))

        async def body_iter():
            # The stack owns both the capacity lease and the budget reservation.
            # Exiting it here covers completion, provider failure, client
            # disconnect and unexpected exceptions with one release path.
            async with stack:
                try:
                    async for frame in to_sse(
                        router.stream(parsed, observer), parsed, timeline
                    ):
                        yield frame
                finally:
                    finish(timeline.tokens)

        return StreamingResponse(
            body_iter(),
            media_type="text/event-stream",
            headers={
                "cache-control": "no-cache",
                # Without this an intermediate proxy buffers the whole response
                # and the client sees one chunk at the end: streaming that works
                # in dev and silently is not streaming in a container.
                "x-accel-buffering": "no",
                **headers,
            },
        )

    @app.get("/v1/scheduler/stats")
    async def scheduler_stats() -> dict[str, Any]:
        """Live scheduler state. Useful for a demo and for debugging a stuck queue."""
        stats = scheduler.stats()
        return {
            "policy": config.scheduling_policy,
            "max_concurrency": config.max_concurrency,
            "inflight": stats.inflight,
            "shared_pool": {"in_use": stats.shared_inflight, "capacity": stats.shared_capacity},
            "queue_depth": stats.queue_depth,
            "tenants": {
                t.id: {
                    "weight": t.weight,
                    "reserved_concurrency": t.reserved_concurrency,
                    "max_concurrency": t.max_concurrency,
                    "inflight": stats.per_tenant_inflight.get(t.id, 0),
                    "queued": stats.per_tenant_queue_depth.get(t.id, 0),
                    "predicted_output_tokens": {
                        model: round(predictor.estimate(t.id, model, t.max_tokens_cap).p50)
                        for model in providers
                    },
                    "budget": _budget_view(t, ledger, predictor, providers[0]),
                }
                for t in config.tenants
            },
        }

    @app.post("/v1/admin/drain")
    async def start_drain() -> dict[str, Any]:
        """Begin draining without stopping the process.

        Shutdown normally drains from the lifespan hook, but doing it on demand
        is what makes the behaviour demonstrable and testable: a deploy can mark
        the instance unready, watch in-flight work finish, and only then stop it.
        """
        result = await scheduler.drain(config.drain_timeout_s)
        return {
            "drained_cleanly": result.clean,
            "queued_rejected": result.queued_rejected,
            "inflight_at_start": result.inflight_at_start,
            "inflight_remaining": result.inflight_remaining,
            "waited_s": round(result.waited_s, 3),
        }

    @app.get("/v1/providers")
    async def providers_health() -> dict[str, Any]:
        """Per-provider breaker state, error breakdown, and observed latency.

        The first place to look when requests are failing: it separates "this
        provider is broken" from "everything is broken" without reading metrics.
        """
        snapshot = provider_health.snapshot()
        for name, entry in snapshot.items():
            entry["routes_serving"] = [
                model for model in ({*config.routes, *providers})
                if name in app.state.router.candidates(model)
            ]
        return snapshot

    @app.get("/metrics")
    async def metrics() -> Response:
        publish_scheduler_gauges()
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health")
    async def health(response: Response) -> dict[str, Any]:
        """Readiness, not liveness.

        Returns 503 while draining so a load balancer stops sending new traffic
        while in-flight requests are still being finished. The process is still
        working -- it just should not be given anything more.
        """
        draining = scheduler.draining
        if draining:
            response.status_code = 503
        return {
            "status": "draining" if draining else "ok",
            "auth": "required" if auth_required else "disabled (no tenants configured)",
            "policy": config.scheduling_policy,
            "max_concurrency": config.max_concurrency,
            "inflight": scheduler.stats().inflight,
        }

    return app


app = create_app()

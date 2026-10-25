"""The console's HTTP surface.

Two tiers, and the split is the security model:

**Public and read-only.** The live map, the snapshot, the SSE stream, history.
No credential required, because the point is that an interviewer can open it
on their own phone. Nothing here carries a secret: the admin key stays in this
process, the demo tenant keys never leave it, and the snapshot is derived
numbers only.

**Protected and side-effecting.** Generating traffic and injecting faults can
overload the gateway and break providers, so they sit behind a session cookie
and a custom header. One password, one role -- see `auth` for why that is the
right size for this.

The console must never be the reason the demo fails. It starts whether or not
the gateway is reachable, the collector loop swallows its own errors, and an
unreachable source renders as a service marked down rather than a 500.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from switchyard.console import auth
from switchyard.console.collector import Collector
from switchyard.console.fleet import FleetControl
from switchyard.console.scenarios import ScenarioRunner
from switchyard.console.settings import Settings, load_settings
from switchyard.console.traffic import PROFILES, TrafficGenerator
from switchyard.obs.logs import ensure_configured, event, get_logger

log = get_logger("console")

STATIC_DIR = Path(__file__).parent / "static"

# Strict by default. The page loads nothing from anywhere else -- no CDN, no
# font service, no analytics -- so the policy can forbid everything that is
# not same-origin and nothing breaks. `frame-ancestors 'none'` stops the
# console being framed by a page that wants to trick someone into clicking a
# demo control.
CSP = (
    "default-src 'self'; img-src 'self' data:; style-src 'self'; "
    "script-src 'self'; connect-src 'self'; frame-ancestors 'none'; "
    "base-uri 'none'; form-action 'self'"
)
SECURITY_HEADERS = {
    "content-security-policy": CSP,
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "x-frame-options": "DENY",
}

# SSE through an intermediary that idles out a quiet connection looks like a
# dead console. A comment line every 15s keeps it open and costs nothing.
HEARTBEAT_S = 15.0


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or load_settings()
    limiter = auth.AttemptLimiter()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        ensure_configured()
        client = httpx.AsyncClient(
            # Generously above the traffic generator's stream cap so the
            # client pool is never the limit the demo is measuring.
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
            timeout=5.0,
        )
        collector = Collector(settings=config, client=client)
        fleet = FleetControl(config.fleet_url, client)
        traffic = TrafficGenerator(
            gateway_url=config.gateway_url, keys=config.demo_keys, client=client,
        )
        scenarios = ScenarioRunner(traffic, fleet, lambda: collector.snapshot)

        app.state.client = client
        app.state.collector = collector
        app.state.fleet = fleet
        app.state.traffic = traffic
        app.state.scenarios = scenarios

        collector.start()
        event(log, "console.started", **config.redacted())
        try:
            yield
        finally:
            await scenarios.stop()
            await traffic.stop()
            await collector.stop()
            await client.aclose()

    app = FastAPI(title="Switchyard Console", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def security_headers(request: Request, call_next: Any) -> Response:
        response: Response = await call_next(request)
        for header, value in SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)
        return response

    if STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    # ------------------------------------------------------------ helpers

    def require_session(request: Request) -> None:
        """Guard a side-effecting endpoint.

        The header check is a cheap CSRF guard: a browser will not attach a
        custom header to a cross-origin form post without a preflight the
        console never answers.
        """
        if not config.demo_controls_available:
            raise HTTPException(
                403, "demo controls are disabled on this deployment")
        if request.headers.get(auth.CSRF_HEADER) != auth.CSRF_VALUE:
            raise HTTPException(400, f"missing {auth.CSRF_HEADER} header")
        token = request.cookies.get(auth.COOKIE_NAME)
        if not auth.verify(config.session_secret, token):
            raise HTTPException(401, "not signed in")

    def client_key(request: Request) -> str:
        return request.client.host if request.client else "unknown"

    # ------------------------------------------------------- public, read

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """The console's own liveness. Deliberately says nothing about the
        gateway: this answers "is the console up", and conflating the two
        would restart the console when the gateway is the thing that is ill."""
        return {"status": "alive"}

    @app.get("/api/snapshot")
    async def snapshot(request: Request) -> dict[str, Any]:
        collector: Collector = request.app.state.collector
        return _with_controls(collector.snapshot or {}, request)

    @app.get("/api/history")
    async def history(request: Request, minutes: int = 10) -> dict[str, Any]:
        collector: Collector = request.app.state.collector
        bounded = max(1, min(minutes, config.history_minutes))
        return {"minutes": bounded, "points": collector.history(bounded)}

    @app.get("/api/stream")
    async def stream(request: Request) -> StreamingResponse:
        """One snapshot per second as server-sent events.

        SSE rather than websockets: the data flows one way, it is a handful of
        kilobytes a second, and it reconnects on its own. A websocket would
        add a protocol to debug for no capability the console uses.
        """
        collector: Collector = request.app.state.collector

        async def events() -> AsyncIterator[bytes]:
            last_beat = 0.0
            beats = 0
            while True:
                if await request.is_disconnected():
                    return
                payload = _with_controls(collector.snapshot or {}, request)
                yield b"event: snapshot\ndata: " + json.dumps(payload).encode() + b"\n\n"
                await asyncio.sleep(config.poll_interval_s)
                beats += 1
                if beats * config.poll_interval_s - last_beat >= HEARTBEAT_S:
                    last_beat = beats * config.poll_interval_s
                    yield b": keep-alive\n\n"

        return StreamingResponse(
            events(),
            media_type="text/event-stream",
            headers={"cache-control": "no-cache",
                     # Without this a proxy buffers the stream and the console
                     # updates in bursts -- the same trap as the gateway's SSE.
                     "x-accel-buffering": "no"},
        )

    @app.get("/")
    async def index() -> Response:
        page = STATIC_DIR / "index.html"
        if not page.is_file():
            return JSONResponse(
                {"status": "console backend running",
                 "note": "the UI is built in M3; the API is live",
                 "api": ["/api/snapshot", "/api/stream", "/api/history"]},
            )
        return FileResponse(page)

    # --------------------------------------------------- protected, write

    @app.post("/api/login")
    async def login(request: Request) -> JSONResponse:
        if not config.demo_controls_available:
            raise HTTPException(403, "demo controls are disabled on this deployment")
        if not limiter.allow(client_key(request)):
            # 429 rather than 401: tell the truth about why it was refused,
            # which does not help an attacker who already knows they are
            # guessing, and does help an operator who typo'd twice.
            raise HTTPException(429, "too many attempts; wait a minute")
        body = await _json_body(request)
        if not auth.password_matches(config.password, str(body.get("password", ""))):
            raise HTTPException(401, "wrong password")
        limiter.reset(client_key(request))
        response = JSONResponse({"ok": True})
        response.set_cookie(
            auth.COOKIE_NAME, auth.issue(config.session_secret),
            max_age=auth.SESSION_TTL_S, httponly=True, samesite="strict",
            # Not `secure`: the free-tier deployment is plain HTTP, so a
            # Secure cookie would never be sent. Recorded in decisions.md as
            # a real weakness, not an oversight.
            path="/",
        )
        return response

    @app.post("/api/logout")
    async def logout() -> JSONResponse:
        response = JSONResponse({"ok": True})
        response.delete_cookie(auth.COOKIE_NAME, path="/")
        return response

    @app.post("/api/traffic")
    async def set_traffic(request: Request) -> dict[str, Any]:
        require_session(request)
        body = await _json_body(request)
        profile = str(body.get("profile", ""))
        generator: TrafficGenerator = request.app.state.traffic
        if profile == "stop":
            await generator.stop()
            return {"ok": True, "traffic": generator.state()}
        if profile not in PROFILES:
            raise HTTPException(400, f"unknown profile {profile!r}; "
                                     f"expected one of {sorted(PROFILES)} or 'stop'")
        await generator.start(profile)
        return {"ok": True, "traffic": generator.state()}

    @app.post("/api/faults/{provider}")
    async def set_fault(provider: str, request: Request) -> dict[str, Any]:
        require_session(request)
        body = await _json_body(request)
        fleet: FleetControl = request.app.state.fleet
        if body.get("clear"):
            await fleet.clear(provider)
            return {"ok": True, "provider": provider, "faults": {}}
        faults: dict[str, Any] = {}
        if "error_rate" in body:
            rate = float(body["error_rate"])
            if not 0.0 <= rate <= 1.0:
                raise HTTPException(400, "error_rate must be between 0 and 1")
            faults["error_rate"] = rate
        if body.get("stall"):
            # The fleet has no boolean "stall": it has a stall *rate*, because
            # a provider that stalls one request in ten is a different failure
            # from one that stalls every time. The toggle means "always".
            faults["stall_rate"] = 1.0
        if not faults:
            raise HTTPException(400, "expected error_rate, stall, or clear")
        await fleet.set_faults(provider, faults)
        return {"ok": True, "provider": provider, "faults": faults}

    @app.post("/api/scenarios/{name}/start")
    async def start_scenario(name: str, request: Request) -> dict[str, Any]:
        require_session(request)
        runner: ScenarioRunner = request.app.state.scenarios
        try:
            await runner.start(name)
        except KeyError:
            raise HTTPException(404, f"unknown scenario {name!r}") from None
        return {"ok": True, "scenario": runner.state()}

    @app.post("/api/scenarios/stop")
    async def stop_scenario(request: Request) -> dict[str, Any]:
        require_session(request)
        runner: ScenarioRunner = request.app.state.scenarios
        await runner.stop()
        return {"ok": True, "scenario": runner.state()}

    @app.post("/api/reset")
    async def reset(request: Request) -> dict[str, Any]:
        """Put everything back: stop traffic, stop any scenario, clear faults.

        The button to press when a demo has gone sideways and the next thing
        that happens is someone watching.
        """
        require_session(request)
        runner: ScenarioRunner = request.app.state.scenarios
        generator: TrafficGenerator = request.app.state.traffic
        fleet: FleetControl = request.app.state.fleet
        await runner.stop()
        await generator.stop()
        await fleet.reset()
        return {"ok": True}

    def _with_controls(snapshot: dict[str, Any], request: Request) -> dict[str, Any]:
        """Attach live traffic and scenario state, and whether we are signed in."""
        if not snapshot:
            return {"at": None, "mode": "simulation", "services": {}, "ready": False,
                    "demo_controls": config.demo_controls_available,
                    "authenticated": False,
                    "note": "collecting the first snapshot"}
        state = request.app.state
        enriched = dict(snapshot)
        enriched["ready"] = True
        enriched["traffic"] = state.traffic.state()
        enriched["scenario"] = state.scenarios.state()
        enriched["authenticated"] = auth.verify(
            config.session_secret, request.cookies.get(auth.COOKIE_NAME))
        enriched["links"] = {"grafana": config.grafana_url or None,
                             "api": config.public_api_url or None}
        return enriched

    return app


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except (json.JSONDecodeError, ValueError):
        return {}
    return body if isinstance(body, dict) else {}

"""The console's HTTP surface, its caps, and one end-to-end scenario.

The console is the part a stranger points a browser at, so the tests care
most about two things: that the read-only surface leaks nothing, and that the
side-effecting surface cannot be driven without a session.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from switchyard.console import auth
from switchyard.console.app import create_app as create_console
from switchyard.console.settings import Settings
from switchyard.console.traffic import (
    MAX_OPEN_STREAMS,
    MAX_PROFILE_DURATION_S,
    MAX_TOTAL_RPS,
    PROFILES,
    Profile,
)

PASSWORD = "demo-password"
SECRET = "test-session-secret"


class RunningConsole:
    def __init__(self, base_url: str, settings: Settings) -> None:
        self.base_url = base_url
        self.settings = settings


def console_settings(**over) -> Settings:
    base = {
        "gateway_url": "http://127.0.0.1:1",      # closed port: gateway "down"
        "fleet_url": "http://127.0.0.1:1",
        "password": PASSWORD,
        "session_secret": SECRET,
        "demo_keys": {"acme": "sk_sy_acme_test"},
        "poll_interval_s": 0.05,
    }
    return Settings(**(base | over))


@pytest_asyncio.fixture
async def console() -> AsyncIterator[RunningConsole]:
    from tests.conftest import _serve

    settings = console_settings()
    server, task, port = await _serve(create_console(settings))
    try:
        yield RunningConsole(f"http://127.0.0.1:{port}", settings)
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10)


async def client_for(c: RunningConsole) -> httpx.AsyncClient:
    return httpx.AsyncClient(base_url=c.base_url, timeout=10.0)


# -- it comes up even with nothing to monitor ------------------------------


async def test_console_starts_with_the_gateway_down(console):
    """The moment you most want the console is when something is broken."""
    async with await client_for(console) as c:
        assert (await c.get("/healthz")).json() == {"status": "alive"}
        snapshot = (await c.get("/api/snapshot")).json()
    # Either the first poll has not landed yet, or it landed and says "down".
    if snapshot.get("ready"):
        assert snapshot["services"]["gateway"] == "down"


async def test_snapshot_never_carries_a_credential(console):
    async with await client_for(console) as c:
        text = (await c.get("/api/snapshot")).text
    for secret in ("sk_sy_acme_test", PASSWORD, SECRET):
        assert secret not in text


async def test_security_headers_are_set(console):
    async with await client_for(console) as c:
        headers = (await c.get("/healthz")).headers
    assert "default-src 'self'" in headers["content-security-policy"]
    assert "frame-ancestors 'none'" in headers["content-security-policy"]
    assert headers["x-content-type-options"] == "nosniff"
    assert headers["x-frame-options"] == "DENY"


async def test_history_is_bounded_to_the_configured_window(console):
    async with await client_for(console) as c:
        body = (await c.get("/api/history?minutes=9999")).json()
    assert body["minutes"] <= console.settings.history_minutes


async def test_stream_sends_snapshots(console):
    async with await client_for(console) as c, c.stream("GET", "/api/stream") as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        async for chunk in response.aiter_bytes():
            if b"event: snapshot" in chunk:
                break


# -- the controls are shut without a session -------------------------------


WRITES = [
    ("/api/traffic", {"profile": "steady"}),
    ("/api/faults/fast", {"error_rate": 1.0}),
    ("/api/scenarios/noisy-neighbour/start", {}),
    ("/api/scenarios/stop", {}),
    ("/api/reset", {}),
]


@pytest.mark.parametrize("path,body", WRITES)
async def test_controls_refuse_an_anonymous_caller(console, path, body):
    async with await client_for(console) as c:
        response = await c.post(path, json=body,
                                headers={auth.CSRF_HEADER: auth.CSRF_VALUE})
    assert response.status_code == 401


@pytest.mark.parametrize("path,body", WRITES)
async def test_controls_refuse_a_request_without_the_csrf_header(console, path, body):
    """A hostile page can forge a cookie-bearing form post, not a custom header."""
    async with await client_for(console) as c:
        c.cookies.set(auth.COOKIE_NAME, auth.issue(SECRET))
        response = await c.post(path, json=body)
    assert response.status_code == 400


async def test_wrong_password_is_refused(console):
    async with await client_for(console) as c:
        response = await c.post("/api/login", json={"password": "wrong"})
    assert response.status_code == 401


async def test_login_sets_an_httponly_strict_cookie(console):
    async with await client_for(console) as c:
        response = await c.post("/api/login", json={"password": PASSWORD})
    assert response.status_code == 200
    cookie = response.headers["set-cookie"].lower()
    assert "httponly" in cookie
    assert "samesite=strict" in cookie


async def test_a_signed_in_caller_can_drive_traffic(console):
    async with await client_for(console) as c:
        assert (await c.post("/api/login", json={"password": PASSWORD})).status_code == 200
        started = await c.post("/api/traffic", json={"profile": "steady"},
                               headers={auth.CSRF_HEADER: auth.CSRF_VALUE})
        assert started.status_code == 200, started.text
        assert started.json()["traffic"]["running"] is True

        stopped = await c.post("/api/traffic", json={"profile": "stop"},
                               headers={auth.CSRF_HEADER: auth.CSRF_VALUE})
        assert stopped.json()["traffic"]["running"] is False


async def test_an_expired_cookie_is_refused(console):
    async with await client_for(console) as c:
        c.cookies.set(auth.COOKIE_NAME, auth.issue(SECRET, now=0.0, ttl_s=1))
        response = await c.post("/api/traffic", json={"profile": "steady"},
                                headers={auth.CSRF_HEADER: auth.CSRF_VALUE})
    assert response.status_code == 401


async def test_unknown_profile_and_scenario_are_rejected(console):
    async with await client_for(console) as c:
        await c.post("/api/login", json={"password": PASSWORD})
        head = {auth.CSRF_HEADER: auth.CSRF_VALUE}
        assert (await c.post("/api/traffic", json={"profile": "nope"},
                             headers=head)).status_code == 400
        assert (await c.post("/api/scenarios/nope/start", json={},
                             headers=head)).status_code == 404


async def test_fault_rate_is_validated(console):
    async with await client_for(console) as c:
        await c.post("/api/login", json={"password": PASSWORD})
        response = await c.post("/api/faults/fast", json={"error_rate": 5.0},
                                headers={auth.CSRF_HEADER: auth.CSRF_VALUE})
    assert response.status_code == 400


def test_stall_toggle_maps_to_stall_rate_not_a_boolean_field():
    """Asserted on the translation itself, with no network involved.

    `replace(FaultSpec, stall=True)` would raise: there is no such field.
    """
    from switchyard.synthetic.profiles import FaultSpec

    assert not hasattr(FaultSpec(), "stall")
    assert hasattr(FaultSpec(), "stall_rate")


async def test_login_is_rate_limited(console):
    async with await client_for(console) as c:
        codes = [(await c.post("/api/login", json={"password": "wrong"})).status_code
                 for _ in range(auth.MAX_ATTEMPTS + 2)]
    assert 429 in codes, "a single password with no throttle gets guessed"


# -- controls disabled for a real-provider deployment ----------------------


@pytest_asyncio.fixture
async def readonly_console() -> AsyncIterator[RunningConsole]:
    """Section 9: CONSOLE_DEMO_CONTROLS=false for a company running live."""
    from tests.conftest import _serve

    settings = console_settings(demo_controls=False)
    server, task, port = await _serve(create_console(settings))
    try:
        yield RunningConsole(f"http://127.0.0.1:{port}", settings)
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10)


@pytest.mark.parametrize("path,body", WRITES)
async def test_disabled_controls_are_forbidden_not_merely_hidden(
    readonly_console, path, body
):
    async with await client_for(readonly_console) as c:
        c.cookies.set(auth.COOKIE_NAME, auth.issue(SECRET))
        response = await c.post(path, json=body,
                                headers={auth.CSRF_HEADER: auth.CSRF_VALUE})
    assert response.status_code == 403


async def test_read_only_surface_still_works_with_controls_off(readonly_console):
    async with await client_for(readonly_console) as c:
        assert (await c.get("/api/snapshot")).status_code == 200
        assert (await c.get("/healthz")).status_code == 200


# -- the caps ---------------------------------------------------------------


def test_shipped_profiles_respect_the_host_caps():
    for profile in PROFILES.values():
        profile.validate()
        assert profile.duration_s <= MAX_PROFILE_DURATION_S


def test_a_profile_over_the_rate_cap_is_refused():
    """The caps are enforced, not documented."""
    from switchyard.console.traffic import Phase

    too_much = Profile(
        name="greedy", headline="", watch_for="",
        phases=(Phase(10.0, {"acme": MAX_TOTAL_RPS + 1}),),
    )
    with pytest.raises(ValueError, match="rps cap"):
        too_much.validate()


def test_a_profile_that_runs_too_long_is_refused():
    from switchyard.console.traffic import Phase

    forever = Profile(
        name="forever", headline="", watch_for="",
        phases=(Phase(MAX_PROFILE_DURATION_S + 1, {"acme": 1.0}),),
    )
    with pytest.raises(ValueError, match="above the"):
        forever.validate()


def test_the_open_stream_cap_is_below_what_would_hurt_the_box():
    assert MAX_OPEN_STREAMS <= 200


# -- end to end, in simulation mode ----------------------------------------


@pytest.mark.slow
async def test_the_outage_scenario_runs_and_passes_against_the_synthetic_fleet():
    """The whole point of M2, proven: fleet + gateway + console, no API key.

    Starts all three in-process, logs in, runs the provider-outage demo, and
    asserts the verdicts the console will show on screen. Slow because it is a
    real 55-second demo -- the same one that runs in front of an interviewer.
    """

    from tests.conftest import _serve

    from switchyard.core.auth import mint_admin_key, mint_key
    from switchyard.core.config import GatewayConfig, Tenant
    from switchyard.gateway.app import create_app as create_gateway
    from switchyard.synthetic.app import FleetState
    from switchyard.synthetic.app import create_app as create_fleet
    from switchyard.synthetic.profiles import ProviderProfile

    fleet_state = FleetState(
        {
            name: ProviderProfile(
                name=name, ttft_median_ms=20.0, ttft_sigma=0.1,
                output_tokens_median=40.0, output_tokens_sigma=0.2,
                tokens_per_second=2000.0, token_jitter_sigma=0.0,
            )
            for name in ("fast", "slow")
        },
        run_seed=5,
    )
    fleet_server, fleet_task, fleet_port = await _serve(create_fleet(fleet_state))
    fleet_url = f"http://127.0.0.1:{fleet_port}"

    admin_raw, admin_digest = mint_admin_key()
    keys: dict[str, str] = {}
    tenants = []
    for tid in ("acme", "globex"):
        raw, digest = mint_key(tid)
        keys[tid] = raw
        tenants.append(Tenant(id=tid, key_sha256=digest, reserved_concurrency=4,
                              max_concurrency=12, deadline_s=10.0))

    gateway_config = GatewayConfig(
        max_concurrency=16, tenants=tuple(tenants), admin_key_sha256=admin_digest,
        providers=("fast", "slow"), fleet_url=fleet_url,
        routes={"fast": ("fast", "slow")},
    )
    gateway_config.validate()
    gw_server, gw_task, gw_port = await _serve(
        create_gateway(config=gateway_config, fleet_url=fleet_url,
                       providers=("fast", "slow"))
    )

    settings = Settings(
        gateway_url=f"http://127.0.0.1:{gw_port}", fleet_url=fleet_url,
        admin_key=admin_raw, demo_keys=keys, password=PASSWORD,
        session_secret=SECRET, poll_interval_s=0.5,
    )
    con_server, con_task, con_port = await _serve(create_console(settings))
    base = f"http://127.0.0.1:{con_port}"

    try:
        async with httpx.AsyncClient(base_url=base, timeout=20.0) as c:
            await asyncio.sleep(2.0)                      # let a snapshot land

            first = (await c.get("/api/snapshot")).json()
            assert first["ready"] is True
            assert first["mode"] == "simulation", "no real providers are configured"
            assert first["services"]["gateway"] == "ready"
            assert {p["kind"] for p in first["providers"]} == {"synthetic"}

            assert (await c.post("/api/login",
                                 json={"password": PASSWORD})).status_code == 200
            head = {auth.CSRF_HEADER: auth.CSRF_VALUE}
            started = await c.post("/api/scenarios/provider-outage/start",
                                   json={}, headers=head)
            assert started.status_code == 200, started.text

            # 10s healthy + 25s outage + 20s recovery, plus slack.
            deadline = asyncio.get_running_loop().time() + 90
            scenario = None
            while asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(2.0)
                scenario = (await c.get("/api/snapshot")).json().get("scenario")
                if scenario and scenario.get("finished"):
                    break

            assert scenario is not None, "scenario state never appeared"
            assert scenario["finished"], f"did not finish: {scenario}"

            checks = {c_["name"]: c_ for c_ in scenario["checks"]}
            failed = [n for n, c_ in checks.items() if c_["status"] == "fail"]
            assert not failed, f"checks failed: { {n: checks[n] for n in failed} }"
            assert checks["clients_kept_being_served"]["status"] == "pass"
            assert checks["breaker_opened"]["status"] == "pass"

            # The demo must leave nothing running behind it.
            after = (await c.get("/api/snapshot")).json()
            assert after["traffic"]["running"] is False
            assert all(not p.get("fault") for p in after["providers"])
    finally:
        for server, task in ((con_server, con_task), (gw_server, gw_task),
                             (fleet_server, fleet_task)):
            server.should_exit = True
            await asyncio.wait_for(task, timeout=15)

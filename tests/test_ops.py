"""Operational surface: liveness, readiness, build identity, provider kind.

These are the endpoints the deployment leans on. The split between /livez and
/readyz exists because the two checks have opposite consequences -- a failed
readiness check should stop traffic arriving, a failed liveness check should
restart the container -- and draining is exactly the state where they must
disagree.
"""

from __future__ import annotations

import httpx
import pytest

from switchyard.buildinfo import UNKNOWN_SHA, build_info
from switchyard.core.config import GatewayConfig, ProviderConfig

# -- build identity --------------------------------------------------------


def test_sha_is_dev_when_not_built_by_the_pipeline(monkeypatch):
    monkeypatch.delenv("SWITCHYARD_GIT_SHA", raising=False)
    assert build_info().git_sha == UNKNOWN_SHA


def test_sha_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("SWITCHYARD_GIT_SHA", "3f9c2a1b4d5e6f70811223344556677889900aab")
    info = build_info()
    assert info.git_sha == "3f9c2a1b4d5e6f70811223344556677889900aab"
    assert info.short_sha == "3f9c2a1"


def test_blank_sha_is_treated_as_unset(monkeypatch):
    """An unsubstituted build arg arrives as empty, not missing."""
    monkeypatch.setenv("SWITCHYARD_GIT_SHA", "   ")
    assert build_info().git_sha == UNKNOWN_SHA


def test_started_at_is_utc_and_uptime_does_not_go_backwards():
    info = build_info()
    assert info.started_at.endswith("Z")
    assert info.uptime_s() >= 0


# -- provider kind ---------------------------------------------------------


def test_unconfigured_provider_is_synthetic():
    assert GatewayConfig(providers=("fast",)).kind_of("fast") == "synthetic"


def test_configured_provider_is_external():
    config = GatewayConfig(
        providers=("openai",),
        provider_endpoints={
            "openai": ProviderConfig(
                name="openai", base_url="https://api.openai.com/v1",
                api_key_env="OPENAI_API_KEY",
            )
        },
    )
    config.validate()
    assert config.kind_of("openai") == "external"


# -- the endpoints ---------------------------------------------------------


async def post(gw, path: str, headers: dict[str, str]) -> httpx.Response:
    async with httpx.AsyncClient(timeout=30.0) as c:
        return await c.post(f"{gw.base_url}{path}", headers=headers)


async def get(gw, path: str, headers: dict[str, str] | None = None) -> httpx.Response:
    async with httpx.AsyncClient(timeout=30.0) as c:
        return await c.get(f"{gw.base_url}{path}", headers=headers or {})


async def test_readyz_carries_build_identity(gateway_server):
    body = (await get(gateway_server, "/readyz")).json()
    assert body["status"] == "ok"
    # The deploy smoke test reads exactly these to prove the right commit is up.
    assert body["git_sha"] == build_info().git_sha
    assert body["version"] == build_info().version
    assert body["started_at"].endswith("Z")
    assert body["uptime_s"] >= 0


async def test_health_is_still_an_alias_of_readyz(gateway_server):
    """Existing callers must not break."""
    health = (await get(gateway_server, "/health")).json()
    readyz = (await get(gateway_server, "/readyz")).json()
    assert health.keys() == readyz.keys()
    assert health["status"] == readyz["status"] == "ok"


async def test_livez_needs_no_credentials(gateway_server):
    response = await get(gateway_server, "/livez")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


async def test_build_info_gauge_is_exported(fleet_server):
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config()
    async with serve_gateway(config, fleet_server, keys) as gw:
        text = (await get(gw, "/metrics", gw.admin())).text
    info = build_info()
    assert f'switchyard_build_info{{git_sha="{info.git_sha}",version="{info.version}"}} 1.0' in text


async def test_livez_stays_up_while_readyz_refuses_traffic(fleet_server):
    """The whole point of the split.

    While draining, a load balancer must stop sending work (readiness fails)
    but the orchestrator must not restart the container (liveness holds) --
    restarting would kill the in-flight requests the drain exists to finish.
    """
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config()
    async with serve_gateway(config, fleet_server, keys) as gw:
        assert (await get(gw, "/readyz")).status_code == 200

        drained = await post(gw, "/v1/admin/drain", gw.admin())
        assert drained.status_code == 200

        ready = await get(gw, "/readyz")
        live = await get(gw, "/livez")
        assert ready.status_code == 503
        assert ready.json()["status"] == "draining"
        assert live.status_code == 200, "liveness must survive a drain"
        assert live.json() == {"status": "alive"}

        # The alias has to follow readiness, not liveness.
        assert (await get(gw, "/health")).status_code == 503


async def test_providers_report_kind_without_leaking_the_endpoint(fleet_server):
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config()
    async with serve_gateway(config, fleet_server, keys) as gw:
        response = await get(gw, "/v1/providers", gw.admin())

    body = response.json()
    assert body, "expected at least one provider"
    for name, entry in body.items():
        assert entry["kind"] == "synthetic", name
    # An admin endpoint still must not hand out where traffic goes or which
    # environment variable holds the key.
    text = response.text
    assert "base_url" not in text
    assert "api_key" not in text


@pytest.mark.parametrize("path", ["/livez", "/readyz", "/health"])
async def test_health_endpoints_are_unauthenticated(fleet_server, path):
    """A probe has no credentials, and giving it one would be a credential
    sitting in a Compose file and an AWS health check."""
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config()
    async with serve_gateway(config, fleet_server, keys) as gw:
        assert (await get(gw, path)).status_code in (200, 503)


async def test_an_explicitly_configured_provider_reports_external(fleet_server):
    """Kind is read from configuration, not inferred from the URL.

    This one points at the synthetic fleet but is declared under
    [providers.*], so it reports "external". That is the honest answer: the
    operator has taken responsibility for where it goes, and the console's
    banner should stop claiming pure simulation.
    """
    from dataclasses import replace

    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config()
    config = replace(
        config,
        providers=("quick",),
        provider_endpoints={
            "quick": ProviderConfig(
                name="quick", base_url=f"{fleet_server.base_url}/v1/quick"
            )
        },
    )
    config.validate()

    async with serve_gateway(config, fleet_server, keys, providers=("quick",)) as gw:
        body = (await get(gw, "/v1/providers", gw.admin())).json()

    assert body["quick"]["kind"] == "external"

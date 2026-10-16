"""Tenant spend surviving a restart.

A budget whose enforcement is bounded by the uptime of the process enforcing it
is not a budget. These tests cover the file format, the failure modes that must
refuse to start rather than silently reset everyone to zero, and the end-to-end
property that matters: stop the gateway, start it again, and the tenant is
still charged for what it used.
"""

from __future__ import annotations

import dataclasses
import json

import pytest

from switchyard.core.budget import BudgetLedger
from switchyard.core.config import ConfigError, GatewayConfig, Tenant, render_toml
from switchyard.core.persistence import (
    STATE_VERSION,
    JsonFileStore,
    NullStore,
    StateError,
    store_for,
)


def tenant(tid: str, budget: int | None = 1000) -> Tenant:
    return Tenant(id=tid, key_sha256="0" * 64, budget_tokens=budget)


# --------------------------------------------------------------------------
# The stores


def test_null_store_keeps_nothing(tmp_path):
    store = NullStore()
    store.save({"acme": 500})
    assert store.load() == {}


def test_missing_file_is_a_fresh_start_not_an_error(tmp_path):
    store = JsonFileStore(tmp_path / "nothing-here.json")
    assert store.load() == {}


def test_round_trip(tmp_path):
    store = JsonFileStore(tmp_path / "state.json")
    store.save({"acme": 1234, "globex": 0})
    assert JsonFileStore(tmp_path / "state.json").load() == {"acme": 1234, "globex": 0}


def test_written_file_is_readable_by_a_human(tmp_path):
    """An operator asking what a tenant spent should be able to use `cat`."""
    path = tmp_path / "state.json"
    JsonFileStore(path).save({"acme": 42})
    payload = json.loads(path.read_text())
    assert payload == {"version": STATE_VERSION, "spent": {"acme": 42}}


def test_save_leaves_no_temp_files_behind(tmp_path):
    store = JsonFileStore(tmp_path / "state.json")
    for n in range(5):
        store.save({"acme": n})
    assert [p.name for p in tmp_path.iterdir()] == ["state.json"]


def test_save_creates_missing_parent_directory(tmp_path):
    store = JsonFileStore(tmp_path / "var" / "lib" / "state.json")
    store.save({"acme": 7})
    assert store.load() == {"acme": 7}


def test_existing_file_survives_a_failed_write(tmp_path):
    """A torn state file is worse than a stale one: the stale one still parses."""
    path = tmp_path / "state.json"
    store = JsonFileStore(path)
    store.save({"acme": 100})
    before = path.read_text()

    # A regular file where a parent directory needs to be.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    with pytest.raises(StateError, match="cannot write"):
        JsonFileStore(blocker / "nested" / "state.json").save({"acme": 1})

    assert path.read_text() == before


# --------------------------------------------------------------------------
# Refusing to start on a file we cannot trust


def test_corrupt_json_refuses_rather_than_resetting(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json")
    with pytest.raises(StateError, match="not valid JSON"):
        JsonFileStore(path).load()


def test_future_version_refuses(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"version": STATE_VERSION + 1, "spent": {"acme": 1}}))
    with pytest.raises(StateError, match="version"):
        JsonFileStore(path).load()


def test_non_object_refuses(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("[1, 2, 3]")
    with pytest.raises(StateError, match="does not contain an object"):
        JsonFileStore(path).load()


@pytest.mark.parametrize("bad", [-1, "500", 1.5, True, None])
def test_implausible_spend_refuses(tmp_path, bad):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"version": STATE_VERSION, "spent": {"acme": bad}}))
    with pytest.raises(StateError, match="non-negative integer"):
        JsonFileStore(path).load()


def test_store_for_picks_the_right_store(tmp_path):
    assert isinstance(store_for(None), NullStore)
    assert isinstance(store_for(str(tmp_path / "s.json")), JsonFileStore)


# --------------------------------------------------------------------------
# The ledger side


def test_restore_applies_to_configured_tenants():
    ledger = BudgetLedger.from_tenants((tenant("acme"), tenant("globex")))
    applied = ledger.restore({"acme": 400, "globex": 100})
    assert applied == {"acme": 400, "globex": 100}
    assert ledger.remaining("acme") == 600


def test_restore_ignores_tenants_that_are_gone():
    """Dropping a tenant from the config must not crash startup."""
    ledger = BudgetLedger.from_tenants((tenant("acme"),))
    assert ledger.restore({"acme": 400, "departed": 999}) == {"acme": 400}


def test_restored_spend_is_enforced():
    """The point of all this: a recovered total still bounds what can be spent."""
    ledger = BudgetLedger.from_tenants((tenant("acme", budget=1000),))
    ledger.restore({"acme": 900})
    with ledger.reserve("acme", 500) as reservation:
        # Clamped to the 100 tokens actually left, not the 500 requested.
        assert reservation.effective_max_tokens == 100
        assert reservation.clamped


def test_restored_exhaustion_refuses_immediately():
    ledger = BudgetLedger.from_tenants((tenant("acme", budget=1000),))
    ledger.restore({"acme": 1000})
    assert not ledger.has_headroom("acme")


def test_dirty_tracks_spend_not_reservations():
    ledger = BudgetLedger.from_tenants((tenant("acme"),))
    assert not ledger.dirty

    with ledger.reserve("acme", 100) as reservation:
        assert not ledger.dirty, "holding a reservation is not spend"
        reservation.actual = 0
    assert not ledger.dirty, "a request that produced nothing costs nothing"

    with ledger.reserve("acme", 100) as reservation:
        reservation.actual = 25
    assert ledger.dirty
    assert ledger.spent_totals() == {"acme": 25}


# --------------------------------------------------------------------------
# Configuration


def test_state_file_is_optional():
    GatewayConfig().validate()


def test_flush_interval_must_be_positive():
    with pytest.raises(ConfigError, match="state_flush_interval_s"):
        GatewayConfig(state_file="s.json", state_flush_interval_s=0).validate()


def test_blank_state_file_is_rejected():
    with pytest.raises(ConfigError, match="state_file"):
        GatewayConfig(state_file="   ").validate()


def test_rendered_config_does_not_carry_the_state_file():
    """`verify` runs a copy of the config; it must not charge real budgets."""
    rendered = render_toml(GatewayConfig(state_file="/var/lib/switchyard/state.json"))
    assert "state_file" not in rendered


# --------------------------------------------------------------------------
# End to end: the property the whole module exists for


async def test_spend_survives_a_gateway_restart(tmp_path, fleet_server):
    """Stop the gateway, start it again, and the tenant is still charged."""
    import httpx
    from tests.conftest import build_tenant_config, serve_gateway

    state = tmp_path / "state.json"
    config, keys = build_tenant_config(max_concurrency=8, budget_tokens=100_000)
    config = dataclasses.replace(
        config, state_file=str(state), state_flush_interval_s=0.05
    )
    body = {"model": "quick", "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 64}

    async def spend_once(gw) -> None:
        async with httpx.AsyncClient(timeout=30.0) as c:
            response = await c.post(f"{gw.base_url}/v1/chat/completions",
                                    json=body, headers=gw.auth("alpha"))
            assert response.status_code == 200

    async def spent(gw) -> int:
        async with httpx.AsyncClient(timeout=30.0) as c:
            stats = await c.get(f"{gw.base_url}/v1/scheduler/stats", headers=gw.admin())
        return stats.json()["tenants"]["alpha"]["budget"]["spent"]

    async with serve_gateway(config, fleet_server, keys) as gw:
        await spend_once(gw)
        first_run_spent = await spent(gw)

    assert first_run_spent > 0, "the request should have cost something"
    # Written during drain, so it does not depend on the flush interval racing.
    assert state.exists()

    async with serve_gateway(config, fleet_server, keys) as gw:
        assert await spent(gw) == first_run_spent, "a restart refunded the tenant"
        await spend_once(gw)
        assert await spent(gw) > first_run_spent


async def test_without_a_state_file_a_restart_starts_fresh(tmp_path, fleet_server):
    """The documented default, asserted so it cannot change by accident."""
    import httpx
    from tests.conftest import build_tenant_config, serve_gateway

    config, keys = build_tenant_config(max_concurrency=8, budget_tokens=100_000)
    body = {"model": "quick", "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 64}

    async def run() -> int:
        async with serve_gateway(config, fleet_server, keys) as gw:
            async with httpx.AsyncClient(timeout=30.0) as c:
                await c.post(f"{gw.base_url}/v1/chat/completions",
                             json=body, headers=gw.auth("alpha"))
                stats = await c.get(f"{gw.base_url}/v1/scheduler/stats",
                                    headers=gw.admin())
            return stats.json()["tenants"]["alpha"]["budget"]["spent"]

    assert await run() == await run()


async def test_a_corrupt_state_file_stops_the_gateway_starting(tmp_path):
    """Refusing to start beats starting with everyone's budget reset to zero."""
    from switchyard.gateway.app import create_app as create_gateway

    state = tmp_path / "state.json"
    state.write_text("{ truncated")
    config = GatewayConfig(state_file=str(state), tenants=(tenant("acme"),))
    with pytest.raises(StateError):
        create_gateway(config=config)

"""Driving the synthetic fleet's fault controls from the console.

Every test here is a regression. The first version of `FleetControl` cleared
faults by sending `{"faults": {}}`, which the fleet applies as a patch -- an
empty patch changes nothing. The provider stayed broken, the breaker's
cooldown kept doubling on each failed probe, and the outage demo ended on a
provider that had never recovered. It looked like a flaky test. It was a bug
that would have failed live, on stage.
"""

from __future__ import annotations

import httpx
import pytest

from switchyard.console.fleet import FAULT_RATES, FleetControl


async def test_profiles_returns_the_map_not_the_envelope(fleet_server):
    """The endpoint wraps profiles beside the run seed; callers want the map."""
    async with httpx.AsyncClient(timeout=10.0) as c:
        profiles = await FleetControl(fleet_server.base_url, c).profiles()
    assert "quick" in profiles, profiles
    assert "run_seed" not in profiles
    assert "faults" in profiles["quick"]


async def test_injecting_a_fault_shows_up_in_the_profile(fleet_server):
    async with httpx.AsyncClient(timeout=10.0) as c:
        fleet = FleetControl(fleet_server.base_url, c)
        await fleet.set_faults("quick", {"error_rate": 1.0})
        profiles = await fleet.profiles()
    assert profiles["quick"]["faults"]["error_rate"] == 1.0


async def test_clear_actually_clears(fleet_server):
    """The bug: an empty patch is a no-op, so the provider stayed broken."""
    async with httpx.AsyncClient(timeout=10.0) as c:
        fleet = FleetControl(fleet_server.base_url, c)
        await fleet.set_faults("quick", {"error_rate": 1.0, "stall_rate": 0.5})
        await fleet.clear("quick")
        profiles = await fleet.profiles()

    faults = profiles["quick"]["faults"]
    for rate in FAULT_RATES:
        assert faults[rate] == 0.0, f"{rate} survived the clear"
    assert FleetControl.active_faults(profiles["quick"]) is None


async def test_an_empty_patch_is_a_no_op_and_that_is_why_clear_is_explicit(fleet_server):
    """Pins the fleet's semantics, so a future refactor cannot quietly undo this."""
    async with httpx.AsyncClient(timeout=10.0) as c:
        fleet = FleetControl(fleet_server.base_url, c)
        await fleet.set_faults("quick", {"error_rate": 1.0})
        await fleet.set_faults("quick", {})          # deliberately the broken call
        profiles = await fleet.profiles()
    assert profiles["quick"]["faults"]["error_rate"] == 1.0, (
        "if this ever becomes 0.0 the fleet changed to replace-semantics"
    )


async def test_reset_restores_every_provider(fleet_server):
    async with httpx.AsyncClient(timeout=10.0) as c:
        fleet = FleetControl(fleet_server.base_url, c)
        await fleet.set_faults("quick", {"error_rate": 1.0})
        await fleet.reset()
        profiles = await fleet.profiles()
    assert profiles["quick"]["faults"]["error_rate"] == 0.0


# -- what counts as a fault ------------------------------------------------


def test_a_healthy_provider_reports_no_fault():
    """error_class is always populated -- it says what kind of error *would*
    be returned. Treating it as a fault paints every provider red."""
    healthy = {"faults": {"error_rate": 0.0, "error_class": "server_error",
                          "abort_rate": 0.0, "abort_after_chunks": 20,
                          "stall_rate": 0.0, "stall_after_chunks": 10}}
    assert FleetControl.active_faults(healthy) is None


def test_only_the_rates_are_reported_as_faults():
    broken = {"faults": {"error_rate": 1.0, "error_class": "server_error",
                         "abort_rate": 0.0, "abort_after_chunks": 20,
                         "stall_rate": 0.0}}
    assert FleetControl.active_faults(broken) == {"error_rate": 1.0}


@pytest.mark.parametrize("profile", [None, {}, {"faults": None}, "nonsense"])
def test_malformed_profiles_do_not_raise(profile):
    """A console must not 500 because the fleet answered oddly."""
    assert FleetControl.active_faults(profile) is None

"""Driving the synthetic fleet's fault-injection API.

Kept separate from the scenario runner because the console exposes manual
fault toggles as well as guided demos, and both need the same four calls.

This is the one part of the console that only exists in simulation mode. A
deployment with real providers has nothing to inject into -- you cannot ask
OpenAI to start returning 5xx -- which is exactly why the synthetic fleet
earns its place: it makes failure a thing you can demonstrate on demand
rather than describe.
"""

from __future__ import annotations

from typing import Any

import httpx

CONTROL_TIMEOUT_S = 3.0

# The fleet's fault spec separates *whether* a fault fires (three rates) from
# *how* it behaves when it does (error_class, the chunk counts, stall_seconds).
# Only the rates say something is wrong, so only these are set to clear a
# provider and only these are reported as an injected fault.
FAULT_RATES = ("error_rate", "abort_rate", "stall_rate")


class FleetControl:
    """Thin client for the fleet's private control endpoints."""

    def __init__(self, base_url: str, client: httpx.AsyncClient) -> None:
        self._base = base_url.rstrip("/")
        self._client = client

    async def profiles(self) -> dict[str, Any]:
        """Profiles keyed by provider name.

        The endpoint wraps them alongside the run seed; callers want the map.
        """
        r = await self._client.get(f"{self._base}/control/profiles",
                                   timeout=CONTROL_TIMEOUT_S)
        r.raise_for_status()
        body = r.json()
        profiles = body.get("profiles", {}) if isinstance(body, dict) else {}
        return profiles if isinstance(profiles, dict) else {}

    async def set_faults(self, provider: str, faults: dict[str, Any]) -> None:
        """Inject faults on one provider.

        The endpoint applies a *patch*, so an empty mapping changes nothing.
        Use `clear` to make a provider healthy -- sending `{}` here is a
        silent no-op, which is how an earlier version of this file managed to
        "recover" a provider that was still failing every request.
        """
        r = await self._client.put(
            f"{self._base}/control/profiles/{provider}",
            json={"faults": faults}, timeout=CONTROL_TIMEOUT_S,
        )
        r.raise_for_status()

    async def clear(self, provider: str) -> None:
        """Make a provider healthy by zeroing every fault rate explicitly."""
        await self.set_faults(provider, dict.fromkeys(FAULT_RATES, 0.0))

    @staticmethod
    def active_faults(profile: Any) -> dict[str, Any] | None:
        """Which faults are actually firing on this provider, if any.

        Reads only the rates. `error_class` is always populated -- it says
        what *kind* of error would be returned if one were -- so treating a
        non-empty value as a fault would paint every healthy provider red.
        """
        if not isinstance(profile, dict):
            return None
        faults = profile.get("faults")
        if not isinstance(faults, dict):
            return None
        active = {name: faults[name] for name in FAULT_RATES if faults.get(name)}
        return active or None

    async def reset(self) -> None:
        """Return every provider to its configured profile."""
        r = await self._client.post(f"{self._base}/control/reset",
                                    timeout=CONTROL_TIMEOUT_S)
        r.raise_for_status()

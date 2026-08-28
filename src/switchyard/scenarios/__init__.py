"""Runnable product scenarios.

Each one sets up a recognisable situation, breaks something on purpose, shows
what the gateway does about it, and ends with a verdict measured from that run.
They need no LLM API key: traffic goes to the built-in synthetic provider.
"""

from __future__ import annotations

from switchyard.scenarios import noisy_neighbour, provider_outage

SCENARIOS = {
    noisy_neighbour.NAME: noisy_neighbour,
    provider_outage.NAME: provider_outage,
}


def names() -> list[str]:
    return sorted(SCENARIOS)

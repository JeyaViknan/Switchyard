"""What this process is, so a deployment can be identified from the outside.

A deploy pipeline that cannot answer "which commit is actually serving?" is
asking you to trust it. The image is built with `--build-arg GIT_SHA=<sha>`,
which becomes `SWITCHYARD_GIT_SHA` in the environment; `/readyz` reports it and
the smoke test compares it against the SHA that was meant to go out. A rollback
is confirmed the same way.

Outside a built image there is no commit to report, so the sha reads `dev`.
That is deliberately not a lie about an unknown value -- `dev` says "not built
by the pipeline", which is exactly what a laptop run is.

Uptime comes from `time.monotonic()` rather than the wall clock, because the
wall clock can step (NTP correction, a suspended laptop) and produce an uptime
that goes backwards.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from switchyard import __version__

# Captured once, at import, which is the closest thing to process start that a
# module can observe.
_STARTED_AT = datetime.now(UTC)
_STARTED_MONOTONIC = time.monotonic()

UNKNOWN_SHA = "dev"


@dataclass(frozen=True, slots=True)
class BuildInfo:
    """Identity of the running process."""

    version: str
    git_sha: str
    started_at: str

    @property
    def short_sha(self) -> str:
        """First 7 characters, the form a human reads and GitHub links to."""
        return self.git_sha[:7]

    def uptime_s(self) -> float:
        return round(time.monotonic() - _STARTED_MONOTONIC, 1)

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "git_sha": self.git_sha,
            "started_at": self.started_at,
            "uptime_s": self.uptime_s(),
        }


def build_info() -> BuildInfo:
    """Read build identity from the environment.

    Read on each call rather than cached at import so that tests can set the
    variable without reloading the module. The cost is one dict lookup.
    """
    sha = os.environ.get("SWITCHYARD_GIT_SHA", "").strip() or UNKNOWN_SHA
    return BuildInfo(
        version=__version__,
        git_sha=sha,
        started_at=_STARTED_AT.isoformat().replace("+00:00", "Z"),
    )

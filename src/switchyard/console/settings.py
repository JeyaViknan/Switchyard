"""Console configuration, entirely from the environment.

Every value has a working default for a laptop, because the demo has to run
from a clean checkout with no credentials and no AWS. The deployment overrides
them from root-only env files written out of SSM.

The console never crashes on missing configuration. A gateway that is down is
something to display, not a reason to exit -- a console that crash-loops when
the thing it monitors is unhealthy is useless exactly when it is needed. The
only genuinely fatal case is being asked to protect the demo controls with no
password, and that is refused at startup rather than silently serving them
open.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any


class ConsoleConfigError(Exception):
    """The console cannot start safely with this configuration."""


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _flag(name: str, default: bool) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class Settings:
    gateway_url: str = "http://127.0.0.1:8000"
    fleet_url: str = "http://127.0.0.1:8100"
    prometheus_url: str = ""
    grafana_url: str = ""
    public_api_url: str = ""

    # Held server-side and never sent to the browser.
    admin_key: str = ""
    demo_keys: dict[str, str] = field(default_factory=dict)

    password: str = ""
    session_secret: str = ""

    # Section 9: a company running real providers turns the demo controls off.
    # Traffic generation and fault injection are demo instruments, not
    # something to leave reachable in front of real traffic.
    demo_controls: bool = True

    git_sha: str = "dev"
    deployed_at: str = ""
    deploy_run_url: str = ""

    poll_interval_s: float = 1.0
    history_minutes: int = 10

    @property
    def demo_controls_available(self) -> bool:
        """Controls need both the switch on and a key to drive traffic with."""
        return self.demo_controls and bool(self.demo_keys)

    def redacted(self) -> dict[str, Any]:
        """Safe to log: shape and presence, never values."""
        return {
            "gateway_url": self.gateway_url,
            "fleet_url": self.fleet_url,
            "admin_key": "set" if self.admin_key else "unset",
            "demo_keys": sorted(self.demo_keys),
            "password": "set" if self.password else "unset",
            "demo_controls": self.demo_controls_available,
            "git_sha": self.git_sha,
        }


def load_settings() -> Settings:
    """Read settings from the environment, validating only what must be."""
    raw_keys = _env("CONSOLE_DEMO_KEYS")
    demo_keys: dict[str, str] = {}
    if raw_keys:
        try:
            parsed = json.loads(raw_keys)
        except json.JSONDecodeError as exc:
            raise ConsoleConfigError(
                f"CONSOLE_DEMO_KEYS is not valid JSON ({exc}). Expected "
                f'{{"tenant": "sk_sy_..."}}.'
            ) from exc
        if not isinstance(parsed, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in parsed.items()
        ):
            raise ConsoleConfigError(
                'CONSOLE_DEMO_KEYS must be an object of {"tenant": "key"} strings'
            )
        demo_keys = parsed

    settings = Settings(
        gateway_url=_env("CONSOLE_GATEWAY_URL", "http://127.0.0.1:8000").rstrip("/"),
        fleet_url=_env("CONSOLE_FLEET_URL", "http://127.0.0.1:8100").rstrip("/"),
        prometheus_url=_env("CONSOLE_PROMETHEUS_URL").rstrip("/"),
        grafana_url=_env("CONSOLE_GRAFANA_URL").rstrip("/"),
        public_api_url=_env("CONSOLE_PUBLIC_API_URL").rstrip("/"),
        admin_key=_env("SWITCHYARD_ADMIN_KEY"),
        demo_keys=demo_keys,
        password=_env("CONSOLE_PASSWORD"),
        session_secret=_env("CONSOLE_SESSION_SECRET"),
        demo_controls=_flag("CONSOLE_DEMO_CONTROLS", True),
        git_sha=_env("SWITCHYARD_GIT_SHA", "dev") or "dev",
        deployed_at=_env("DEPLOYED_AT"),
        deploy_run_url=_env("DEPLOY_RUN_URL"),
    )

    # Serving controls that can flood the gateway and break providers, with no
    # password, is the one configuration worth refusing to start on.
    if settings.demo_controls_available and not settings.password:
        raise ConsoleConfigError(
            "demo controls are enabled but CONSOLE_PASSWORD is unset: traffic "
            "generation and fault injection would be open to anyone who can "
            "reach the console. Set CONSOLE_PASSWORD, or set "
            "CONSOLE_DEMO_CONTROLS=false to serve a read-only console."
        )
    return settings

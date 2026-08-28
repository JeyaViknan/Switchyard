"""Tenant and gateway configuration.

Tenants live in a TOML file rather than a database. They are configuration --
edited deliberately, reviewed, and deployed -- not runtime data, and there is no
admin API that would need to write them. A database here would add a failure
domain, a migration story, and an async query on the request path in exchange
for nothing the scheduler needs. When tenants become self-service, this moves.

Budgets are denominated in tokens, not currency. Tokens are the resource the
scheduler actually allocates; a per-model price is attached for reporting so
cost is still visible without the gateway carrying a pricing subsystem whose
numbers would be stale the week after they were written.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_CONFIG_PATH = "switchyard.toml"


class ConfigError(ValueError):
    """Raised for a malformed configuration, with the offending field named."""


@dataclass(frozen=True, slots=True)
class Tenant:
    """One tenant's identity and its share of the gateway's capacity.

    `weight` governs the share of *contended* capacity a tenant receives when
    several are backlogged. `reserved_concurrency` is a floor it can always
    reach regardless of what anyone else is doing, and `max_concurrency` is a
    ceiling that stops one tenant from occupying the whole gateway even when it
    is otherwise idle. The three together are what make a noisy neighbour
    survivable: a floor nobody can take, a ceiling nobody can exceed, and a
    weighted split of whatever is left.
    """

    id: str
    key_sha256: str
    weight: float = 1.0
    reserved_concurrency: int = 0
    max_concurrency: int | None = None
    budget_tokens: int | None = None
    max_queue_depth: int = 128
    deadline_s: float = 60.0
    max_tokens_cap: int = 4096

    def validate(self) -> None:
        if not self.id or not self.id.replace("-", "").replace("_", "").isalnum():
            raise ConfigError(f"tenant id {self.id!r} must be alphanumeric (- and _ allowed)")
        if len(self.key_sha256) != 64:
            raise ConfigError(f"tenant {self.id}: key_sha256 must be a 64-char hex digest")
        if self.weight <= 0:
            raise ConfigError(f"tenant {self.id}: weight must be > 0")
        if self.reserved_concurrency < 0:
            raise ConfigError(f"tenant {self.id}: reserved_concurrency must be >= 0")
        if self.max_concurrency is not None:
            if self.max_concurrency < 1:
                raise ConfigError(f"tenant {self.id}: max_concurrency must be >= 1")
            if self.max_concurrency < self.reserved_concurrency:
                raise ConfigError(
                    f"tenant {self.id}: max_concurrency ({self.max_concurrency}) is below "
                    f"reserved_concurrency ({self.reserved_concurrency}); the tenant could "
                    f"never reach its own guarantee"
                )
        if self.budget_tokens is not None and self.budget_tokens < 0:
            raise ConfigError(f"tenant {self.id}: budget_tokens must be >= 0")
        if self.max_queue_depth < 0:
            raise ConfigError(f"tenant {self.id}: max_queue_depth must be >= 0")
        if self.deadline_s <= 0:
            raise ConfigError(f"tenant {self.id}: deadline_s must be > 0")


@dataclass(frozen=True, slots=True)
class GatewayConfig:
    """Gateway-wide settings.

    `max_concurrency` is the total number of requests that may be in flight to
    providers at once. It is the scarce resource the whole scheduler exists to
    allocate: set it to what the upstream can actually absorb, not to what the
    gateway can hold open.
    """

    max_concurrency: int = 32
    scheduling_policy: str = "drr"
    tenants: tuple[Tenant, ...] = ()
    fleet_url: str = "http://127.0.0.1:8100"
    providers: tuple[str, ...] = ("fast", "slow")

    def validate(self) -> None:
        if self.max_concurrency < 1:
            raise ConfigError("gateway.max_concurrency must be >= 1")
        if self.scheduling_policy not in ("drr", "fifo"):
            raise ConfigError(
                f"gateway.scheduling_policy must be 'drr' or 'fifo', "
                f"got {self.scheduling_policy!r}"
            )
        seen: set[str] = set()
        for tenant in self.tenants:
            tenant.validate()
            if tenant.id in seen:
                raise ConfigError(f"duplicate tenant id {tenant.id!r}")
            seen.add(tenant.id)

        # Reserved capacity that exceeds the total is not a guarantee, it is an
        # overdraft: several tenants would each be promised a floor the gateway
        # cannot simultaneously honour. Better to refuse the config than to
        # discover it under load.
        reserved = sum(t.reserved_concurrency for t in self.tenants)
        if reserved > self.max_concurrency:
            raise ConfigError(
                f"reserved concurrency across tenants ({reserved}) exceeds "
                f"gateway.max_concurrency ({self.max_concurrency}): these "
                f"guarantees cannot all be honoured at once"
            )

    @property
    def tenants_by_id(self) -> dict[str, Tenant]:
        return {t.id: t for t in self.tenants}


def _tenant_from_toml(raw: dict[str, Any]) -> Tenant:
    known = {f for f in Tenant.__slots__}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(
            f"tenant {raw.get('id', '?')!r}: unknown field(s) {sorted(unknown)}. "
            f"Known fields: {sorted(known)}"
        )
    try:
        return Tenant(**raw)
    except TypeError as exc:
        raise ConfigError(f"tenant {raw.get('id', '?')!r}: {exc}") from exc


def load_config(path: str | Path = DEFAULT_CONFIG_PATH) -> GatewayConfig:
    """Load and validate configuration. Raises ConfigError with a usable message."""
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"config file not found: {p}")

    with p.open("rb") as fh:
        raw = tomllib.load(fh)

    gateway_raw = dict(raw.get("gateway", {}))
    tenants = tuple(_tenant_from_toml(dict(t)) for t in raw.get("tenants", []))

    known = {"max_concurrency", "scheduling_policy", "fleet_url", "providers"}
    unknown = set(gateway_raw) - known
    if unknown:
        raise ConfigError(f"[gateway]: unknown field(s) {sorted(unknown)}")
    if "providers" in gateway_raw:
        gateway_raw["providers"] = tuple(gateway_raw["providers"])

    config = GatewayConfig(tenants=tenants, **gateway_raw)
    config.validate()
    return config

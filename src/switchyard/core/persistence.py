"""Durable tenant spend, so a restart does not hand everyone a fresh budget.

The problem this solves
-----------------------
Budgets are the one part of the gateway that represents money. Everything else
in the process is safely ephemeral: queues drain, leases expire, breakers
re-learn provider health within seconds. Spend does not re-learn. A tenant that
had consumed 95% of its allowance before a deploy would come back at zero, and
the hard bound the budget ledger works so carefully to maintain would be
bounded only by the uptime of the process enforcing it.

So the ledger's `spent` totals are written to a file and read back at startup.
Nothing else is: `reserved` covers in-flight requests, and after a restart
nothing is in flight, so persisting it would permanently leak headroom.

Why a JSON file and not a database
----------------------------------
The entire dataset is one integer per tenant -- tens of rows, a few hundred
bytes. A database would add a schema, a migration story and a failure domain to
hold less data than this docstring. The file is written whole and moved into
place with `os.replace`, which is atomic on POSIX and on Windows, so a crash
mid-write leaves the previous version intact rather than a truncated one.

It is also plain text, which matters more than it sounds: an operator asking
"what has this tenant actually spent?" can answer it with `cat`.

The durability bound, stated rather than implied
------------------------------------------------
Writing on every settle would put a filesystem round trip on the completion
path of every request. Instead writes are batched: the gateway flushes on an
interval and once more during drain. A crash can therefore lose up to
`flush_interval_s` of recorded spend -- the tenant is under-charged for
requests that completed inside the last window, never over-charged.

That bound is configurable, and shortening it is the way to tighten it.
Flushing on every settle is deliberately *not* offered: it would put an fsync
on the completion path of every request, making a gateway whose whole purpose
is scheduling throughput wait on a disk a few hundred times a second. A tenant
briefly under-charged after a crash is a far smaller problem than a gateway
that cannot serve.

Nothing here is a distributed ledger. One process owns the file; running two
gateways against the same path will have them overwrite each other, which is
why `switchyard check` warns about it rather than leaving it to be discovered.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

# Bumped only if the on-disk shape changes incompatibly. A file written by a
# newer Switchyard is refused rather than silently misread.
STATE_VERSION = 1


class StateError(Exception):
    """The state file exists but cannot be used."""


class BudgetStore(Protocol):
    """Where tenant spend is kept between runs."""

    def load(self) -> dict[str, int]:
        """Spend totals recorded by a previous run, keyed by tenant id."""
        ...

    def save(self, spent: dict[str, int]) -> None:
        """Persist spend totals, replacing what was there."""
        ...


@dataclass(slots=True)
class NullStore:
    """Keeps nothing. The default, and what the scenarios and tests run with.

    Budgets still work exactly as before within a single run; they simply start
    empty each time. This is the right behaviour for a demo and the wrong
    behaviour for a deployment, which is why the gateway logs which one it is
    using at startup.
    """

    def load(self) -> dict[str, int]:
        return {}

    def save(self, spent: dict[str, int]) -> None:
        return None


@dataclass(slots=True)
class JsonFileStore:
    """Tenant spend in a JSON file, replaced atomically on every write."""

    path: Path
    # Tracked so the gateway can report what it recovered without re-reading.
    loaded: dict[str, int] = field(default_factory=dict)

    def load(self) -> dict[str, int]:
        try:
            raw = self.path.read_text()
        except FileNotFoundError:
            self.loaded = {}
            return {}
        except OSError as exc:
            raise StateError(f"cannot read state file {self.path}: {exc}") from exc

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise StateError(
                f"state file {self.path} is not valid JSON ({exc}). Refusing to start "
                f"rather than silently resetting every tenant's spend to zero. Move the "
                f"file aside to start fresh."
            ) from exc

        if not isinstance(payload, dict):
            raise StateError(f"state file {self.path} does not contain an object")

        version = payload.get("version")
        if version != STATE_VERSION:
            raise StateError(
                f"state file {self.path} has version {version!r}, but this Switchyard "
                f"writes version {STATE_VERSION}. Refusing to guess at its meaning."
            )

        spent = payload.get("spent", {})
        if not isinstance(spent, dict):
            raise StateError(f"state file {self.path}: 'spent' is not an object")

        recovered: dict[str, int] = {}
        for tenant_id, value in spent.items():
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise StateError(
                    f"state file {self.path}: spend for tenant {tenant_id!r} is "
                    f"{value!r}, which is not a non-negative integer"
                )
            recovered[tenant_id] = value

        self.loaded = recovered
        return dict(recovered)

    def save(self, spent: dict[str, int]) -> None:
        """Write the whole file and move it into place.

        Writing in place would leave a truncated file if the process died
        between `truncate` and `write`; a torn state file is worse than a stale
        one, because the stale one still parses.
        """
        payload = {"version": STATE_VERSION, "spent": dict(sorted(spent.items()))}
        body = json.dumps(payload, indent=2, sort_keys=True) + "\n"

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_fd, tmp_name = tempfile.mkstemp(
                dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
            )
        except OSError as exc:
            raise StateError(f"cannot write state file {self.path}: {exc}") from exc

        try:
            with os.fdopen(tmp_fd, "w") as handle:
                handle.write(body)
                handle.flush()
                # Without this the rename can be durable while the contents are
                # not, which on a power loss gives an empty file where a valid
                # one used to be.
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
        except OSError as exc:
            # Best-effort cleanup; the temp file is namespaced so a leftover is
            # identifiable rather than mysterious.
            with contextlib.suppress(OSError):
                os.unlink(tmp_name)
            raise StateError(f"cannot write state file {self.path}: {exc}") from exc


def store_for(path: str | None) -> BudgetStore:
    """The store a configuration asks for: a file if named, otherwise nothing."""
    return NullStore() if path is None else JsonFileStore(Path(path))

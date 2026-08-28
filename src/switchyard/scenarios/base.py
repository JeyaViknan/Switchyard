"""Shared machinery for product scenarios.

A scenario is a demonstration, not a benchmark. It sets up a situation a
developer would recognise, does something visibly bad to the system, shows what
the system does about it, and ends with a verdict drawn from that run. Every
number printed is measured; nothing is precomputed.

Output streams rather than redrawing the screen. A scrolling transcript can be
read back after the fact, survives being piped into a file or CI log, and keeps
the narration next to the numbers it explains -- all of which matter more here
than a live table would.
"""

from __future__ import annotations

import enum
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field

from switchyard.cli.render import Style


class Outcome(enum.StrEnum):
    """Three states, not two.

    SKIP is load-bearing. A configuration with one tenant cannot be checked for
    isolation between tenants, and reporting that as a pass would be a lie while
    reporting it as a failure would be worse. Saying plainly that the check does
    not apply, and why, is the only honest option -- and it often points at the
    configuration gap that matters.
    """

    PASS = "PASS"
    FAIL = "FAIL"
    SKIP = "SKIP"


@dataclass(frozen=True, slots=True)
class Check:
    """One guarantee, and what this run found."""

    name: str
    outcome: Outcome
    detail: str
    explain: str = ""
    recommendation: str = ""

    @property
    def passed(self) -> bool:
        """A skipped check does not fail the run; it was not checked."""
        return self.outcome is not Outcome.FAIL

    @classmethod
    def result(cls, name: str, ok: bool, detail: str, explain: str = "",
               recommendation: str = "") -> Check:
        return cls(name, Outcome.PASS if ok else Outcome.FAIL, detail, explain, recommendation)

    @classmethod
    def skip(cls, name: str, why: str, recommendation: str = "") -> Check:
        return cls(name, Outcome.SKIP, why, recommendation=recommendation)


@dataclass(slots=True)
class ScenarioResult:
    name: str
    checks: list[Check] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def exit_code(self) -> int:
        return 0 if self.passed else 1


class Reporter:
    """Prints the scenario transcript."""

    def __init__(self, style: Style | None = None, stream=None) -> None:
        self.style = style or Style()
        self.stream = stream or sys.stdout
        self._t0 = time.perf_counter()

    def _write(self, text: str = "") -> None:
        print(text, file=self.stream, flush=True)

    @property
    def elapsed(self) -> float:
        return time.perf_counter() - self._t0

    def start(self) -> None:
        self._t0 = time.perf_counter()

    def heading(self, title: str, subtitle: str) -> None:
        self._write()
        self._write(f"  {self.style.bold(title)}  {self.style.dim(subtitle)}")
        self._write()

    def section(self, label: str) -> None:
        self._write(f"  {self.style.dim(label)}")

    def detail(self, text: str) -> None:
        self._write(f"    {text}")

    def event(self, text: str) -> None:
        """A moment the viewer should notice: the fault starting, a breaker tripping."""
        self._write(f"  {self.elapsed:5.1f}s  {self.style.yellow('>>')} {self.style.bold(text)}")

    def status(self, text: str) -> None:
        self._write(f"  {self.elapsed:5.1f}s     {text}")

    def lines(self, rendered: Sequence[str]) -> None:
        """Emit already-formatted lines, e.g. a rendered interpretation block."""
        for line in rendered:
            self._write(line)

    def note(self, text: str) -> None:
        self._write(f"    {self.style.dim(text)}")

    def mark(self, outcome: Outcome) -> str:
        return {
            Outcome.PASS: self.style.green("PASS"),
            Outcome.FAIL: self.style.red("FAIL"),
            Outcome.SKIP: self.style.dim("SKIP"),
        }[outcome]

    def check(self, check: Check, width: int = 48) -> None:
        """One line for the result, indented detail only when it needs acting on."""
        self._write(f"    {self.mark(check.outcome)}  {check.name:<{width}}"
                    f"{self.style.dim(check.detail)}")
        if check.outcome is Outcome.PASS:
            return
        if check.outcome is Outcome.FAIL and check.explain:
            self._write(f"          {check.explain}")
        if check.recommendation:
            self._write(f"          {self.style.dim('-> ' + check.recommendation)}")

    def verdict(self, result: ScenarioResult) -> None:
        self._write()
        self._write(f"  {self.style.dim('Verdict')}")
        for check in result.checks:
            self.check(check)
        self._write()
        if result.passed:
            self._write(f"  {self.style.green('All guarantees held.')}")
        else:
            failed = sum(1 for c in result.checks if not c.passed)
            self._write(f"  {self.style.red(f'{failed} guarantee(s) did not hold.')}")
        for note in result.notes:
            self._write(f"  {self.style.dim(note)}")
        self._write()


def rate(count: int, seconds: float) -> float:
    return count / seconds if seconds > 0 else 0.0


def pct(part: int, whole: int) -> float:
    return (part / whole * 100.0) if whole else 0.0


def summarise_rejections(records: Sequence) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in records:
        if not r.ok:
            key = r.error or f"http_{r.status}"
            out[key] = out.get(key, 0) + 1
    return out


@dataclass(frozen=True, slots=True)
class TenantSpec:
    """A tenant to create for a scenario."""

    id: str
    weight: float = 1.0
    reserved_concurrency: int = 0
    max_queue_depth: int = 128
    deadline_s: float = 20.0
    budget_tokens: int | None = None


def write_scenario_config(
    path, tenants: Sequence[TenantSpec], digests: dict[str, str], admin_digest: str,
    max_concurrency: int, policy: str = "drr",
    routes: dict[str, Sequence[str]] | None = None,
    providers: Sequence[str] = ("fast", "slow"),
    ttft_s: float = 4.0, breaker_min_samples: int = 8, cooldown_s: float = 4.0,
    breaker_window: int = 50,
) -> None:
    """Write a config for the scenario's gateway.

    Scenarios run against a generated configuration rather than the user's, so
    that a demo behaves identically on every machine. `switchyard verify` is the
    command that runs against your own config.
    """
    lines = [
        "[gateway]",
        f"max_concurrency = {max_concurrency}",
        f'scheduling_policy = "{policy}"',
        f'admin_key_sha256 = "{admin_digest}"',
        "providers = [" + ", ".join(f'"{p}"' for p in providers) + "]",
        "",
        "[timeouts]",
        f"ttft_s = {ttft_s}",
        "inter_token_s = 5.0",
        "total_s = 120.0",
        "",
        "[breaker]",
        f"min_samples = {breaker_min_samples}",
        f"window = {breaker_window}",
        f"cooldown_s = {cooldown_s}",
        "half_open_probes = 2",
        "",
    ]
    if routes:
        lines.append("[routes]")
        for model, candidates in routes.items():
            lines.append(f"{model} = [" + ", ".join(f'"{c}"' for c in candidates) + "]")
        lines.append("")
    for t in tenants:
        lines += [
            "[[tenants]]",
            f'id = "{t.id}"',
            f'key_sha256 = "{digests[t.id]}"',
            f"weight = {t.weight}",
            f"reserved_concurrency = {t.reserved_concurrency}",
            f"max_queue_depth = {t.max_queue_depth}",
            f"deadline_s = {t.deadline_s}",
        ]
        if t.budget_tokens is not None:
            lines.append(f"budget_tokens = {t.budget_tokens}")
        lines.append("")
    path.write_text("\n".join(lines))

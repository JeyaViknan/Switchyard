"""Product scenario machinery and wiring.

The scenarios themselves take half a minute each, so the full-length runs are
not part of the suite. What is tested here is that the verdict machinery is
honest -- a failing check must fail the run and the exit code -- and that both
scenarios actually work end to end, using shortened durations.

The end-to-end tests deliberately assert *structure* rather than outcome: with
two seconds of load a guarantee may legitimately not hold, and a test that
demanded PASS would either be flaky or would pressure the thresholds into
meaninglessness.
"""

from __future__ import annotations

import io

import pytest

from switchyard.cli.render import Style
from switchyard.core.config import load_config
from switchyard.scenarios import SCENARIOS, names
from switchyard.scenarios.base import (
    Check,
    Reporter,
    ScenarioResult,
    TenantSpec,
    pct,
    rate,
    summarise_rejections,
    write_scenario_config,
)

PLAIN = Style(enabled=False)


def reporter() -> tuple[Reporter, io.StringIO]:
    buffer = io.StringIO()
    return Reporter(style=PLAIN, stream=buffer), buffer


# -- verdicts --------------------------------------------------------------


def test_a_result_passes_only_when_every_check_passes():
    ok = ScenarioResult("s", [Check("a", True, ""), Check("b", True, "")])
    bad = ScenarioResult("s", [Check("a", True, ""), Check("b", False, "")])
    assert ok.passed and ok.exit_code == 0
    assert not bad.passed and bad.exit_code == 1


def test_verdict_shows_the_measured_detail_for_every_check():
    rep, buf = reporter()
    rep.verdict(ScenarioResult("s", [
        Check("isolation held", True, "queue wait p95 41ms"),
        Check("nothing leaked", False, "3 in flight", "capacity was still held"),
    ]))
    out = buf.getvalue()
    assert "PASS  isolation held" in out
    assert "FAIL  nothing leaked" in out
    assert "queue wait p95 41ms" in out
    assert "capacity was still held" in out, "a failure should explain itself"
    assert "1 guarantee(s) did not hold." in out


def test_a_passing_verdict_says_so():
    rep, buf = reporter()
    rep.verdict(ScenarioResult("s", [Check("a", True, "1.0")]))
    assert "All guarantees held." in buf.getvalue()


def test_transcript_marks_events_distinctly_from_status():
    rep, buf = reporter()
    rep.heading("noisy neighbour", "one tenant floods")
    rep.event("flood starts")
    rep.status("capacity 4/8")
    out = buf.getvalue()
    assert ">>" in out and "flood starts" in out
    assert "capacity 4/8" in out
    assert out.index("flood starts") < out.index("capacity 4/8")


# -- helpers ---------------------------------------------------------------


def test_rate_and_pct_handle_empty_windows():
    assert rate(0, 0) == 0.0
    assert pct(0, 0) == 0.0
    assert rate(10, 2) == 5.0
    assert pct(1, 4) == 25.0


def test_rejections_are_grouped_by_cause():
    class R:
        def __init__(self, ok, error=None, status=200):
            self.ok, self.error, self.status = ok, error, status

    records = [R(True), R(False, "queue_full"), R(False, "queue_full"), R(False, None, 503)]
    assert summarise_rejections(records) == {"queue_full": 2, "http_503": 1}


# -- generated configuration -----------------------------------------------


def test_generated_config_is_valid_and_carries_the_scenario_shape(tmp_path):
    path = tmp_path / "s.toml"
    write_scenario_config(
        path,
        [TenantSpec("quiet", reserved_concurrency=6), TenantSpec("noisy", max_queue_depth=64)],
        {"quiet": "a" * 64, "noisy": "b" * 64}, "c" * 64,
        max_concurrency=12, routes={"fast": ("fast", "slow")}, breaker_window=16,
    )
    config = load_config(path)
    config.validate()

    assert config.max_concurrency == 12
    assert config.admin_key_sha256 == "c" * 64
    assert config.routes == {"fast": ("fast", "slow")}
    assert config.breaker.window == 16
    tenants = {t.id: t for t in config.tenants}
    assert tenants["quiet"].reserved_concurrency == 6
    assert tenants["noisy"].max_queue_depth == 64


def test_generated_config_can_express_a_budget(tmp_path):
    path = tmp_path / "s.toml"
    write_scenario_config(path, [TenantSpec("t", budget_tokens=5000)],
                          {"t": "a" * 64}, "c" * 64, max_concurrency=4)
    config = load_config(path)
    assert config.tenants[0].budget_tokens == 5000


# -- registry --------------------------------------------------------------


def test_both_scenarios_are_registered():
    assert names() == ["noisy-neighbour", "provider-outage"]
    for module in SCENARIOS.values():
        assert callable(module.run)
        assert module.TITLE and module.SUBTITLE


# -- end to end ------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.parametrize("name", ["noisy-neighbour", "provider-outage"])
async def test_scenario_runs_end_to_end_and_produces_a_verdict(name):
    """Shortened, and asserting shape rather than outcome.

    Two seconds of load is not enough for a guarantee to be meaningfully upheld
    or violated, so demanding PASS here would either be flaky or would push the
    real thresholds toward being unfalsifiable.
    """
    rep, buf = reporter()
    module = SCENARIOS[name]
    short = (
        {"baseline_s": 2.0, "flood_s": 3.0, "noisy_rate": 12.0}
        if name == "noisy-neighbour"
        else {"healthy_s": 2.0, "outage_s": 3.0, "recovery_s": 2.0}
    )
    result = await module.run(rep, **short)
    rep.verdict(result)

    transcript = buf.getvalue()
    assert result.checks, "a scenario must produce checks"
    assert result.exit_code in (0, 1)
    assert "Setup" in transcript and "Verdict" in transcript
    assert "no LLM API key needed" in transcript, "the no-key promise should be visible"
    assert all(c.detail for c in result.checks), "every check reports a measured value"


@pytest.mark.slow
async def test_the_outage_scenario_actually_injects_a_failure():
    """The fault has to reach the provider, or the scenario proves nothing."""
    rep, buf = reporter()
    result = await SCENARIOS["provider-outage"].run(
        rep, healthy_s=2.0, outage_s=4.0, recovery_s=2.0
    )
    failures = next(c for c in result.checks if "fallback" in c.name)
    assert "failures recorded" in failures.detail
    assert ">> 'fast' starts returning 5xx" in buf.getvalue()

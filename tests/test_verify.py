"""Configuration verification.

The verifier's job is to read the guarantees out of someone's configuration and
check them, so the tests focus on that reading: an unmade claim must be skipped
rather than passed, and a broken claim must fail with something actionable.

The full run starts services and drives load for about forty seconds, so it is
marked slow. What is exercised here without that cost is the static review, the
configuration copying, and the skip logic -- which is where the judgement lives.
"""

from __future__ import annotations

import io

import pytest

from switchyard.analysis import breaker_failures_needed, interpret, review
from switchyard.cli.render import Style
from switchyard.core.config import GatewayConfig, Tenant, TimeoutPolicy, load_config, render_toml
from switchyard.scenarios.base import Outcome, Reporter
from switchyard.verify import default_model, derive_config, primary_model

DIGEST = "a" * 64
PLAIN = Style(enabled=False)


def tenant(tid: str, **over) -> Tenant:
    return Tenant(**({"id": tid, "key_sha256": DIGEST} | over))


def config(**over) -> GatewayConfig:
    base = {
        "max_concurrency": 12,
        "admin_key_sha256": DIGEST,
        "tenants": (
            tenant("a", reserved_concurrency=4, max_concurrency=8),
            tenant("b", reserved_concurrency=2, max_concurrency=6),
        ),
    }
    cfg = GatewayConfig(**(base | over))
    cfg.validate()
    return cfg


def outcomes(checks) -> dict[str, Outcome]:
    return {c.name: c.outcome for c in checks}


# -- reading the claims out of a configuration -----------------------------


def test_a_healthy_configuration_passes_the_static_review():
    assert set(outcomes(review(config())).values()) == {Outcome.PASS}


def test_isolation_is_skipped_when_there_is_only_one_tenant():
    """Nothing to be isolated from. Passing that would be a lie."""
    checks = outcomes(review(config(tenants=(tenant("solo"),))))
    assert checks["tenants are isolated from each other"] is Outcome.SKIP


def test_isolation_is_skipped_when_no_tenant_claims_a_floor():
    """No guarantee was configured, so there is no guarantee to verify."""
    checks = review(config(tenants=(tenant("a"), tenant("b"))))
    isolation = next(c for c in checks if "isolated from each other" in c.name)
    assert isolation.outcome is Outcome.SKIP
    assert "reserved_concurrency" in isolation.recommendation


def test_a_missing_admin_key_fails_with_the_command_to_fix_it():
    checks = review(config(admin_key_sha256=None))
    admin = next(c for c in checks if "operational endpoints" in c.name)
    assert admin.outcome is Outcome.FAIL
    assert "switchyard keys mint --admin" in admin.recommendation


def test_open_development_mode_is_not_reported_as_insecure():
    """No tenants means no auth is expected; flagging it would be noise."""
    checks = review(GatewayConfig(tenants=()))
    admin = next(c for c in checks if "operational endpoints" in c.name)
    assert admin.outcome is Outcome.PASS


def test_a_tenant_without_a_ceiling_is_reported():
    checks = review(config(
        tenants=(tenant("a", reserved_concurrency=2), tenant("b", max_concurrency=4))
    ))
    ceiling = next(c for c in checks if "whole gateway" in c.name)
    assert ceiling.outcome is Outcome.FAIL
    assert "a" in ceiling.explain


# -- routes ----------------------------------------------------------------


def test_a_model_with_a_fallback_is_found():
    cfg = config(providers=("p1", "p2"), routes={"m": ("p1", "p2"), "other": ("p2",)})
    assert primary_model(cfg) == ("m", ("p1", "p2"))


def test_no_fallback_anywhere_is_reported_as_absent():
    cfg = config(providers=("p1",), routes={"m": ("p1",)})
    assert primary_model(cfg) is None


def test_default_model_falls_back_to_a_provider_name():
    assert default_model(config(providers=("p1",), routes={})) == "p1"
    assert default_model(config(providers=("p1",), routes={"m": ("p1",)})) == "m"


# -- running a copy of the configuration -----------------------------------


def test_derived_config_keeps_every_limit_but_swaps_credentials():
    """The limits are the subject of the check, so they must survive exactly."""
    original = config(
        tenants=(
            tenant("a", reserved_concurrency=4, max_concurrency=8,
                   budget_tokens=1234, weight=3.0, deadline_s=17.0),
        ),
        timeouts=TimeoutPolicy(ttft_s=2.5),
        routes={"m": ("fast", "slow")},
    )
    derived, keys, admin = derive_config(original)

    assert derived.max_concurrency == original.max_concurrency
    assert derived.routes == original.routes
    assert derived.timeouts == original.timeouts
    got = derived.tenants[0]
    want = original.tenants[0]
    assert (got.reserved_concurrency, got.max_concurrency, got.budget_tokens,
            got.weight, got.deadline_s) == (
        want.reserved_concurrency, want.max_concurrency, want.budget_tokens,
        want.weight, want.deadline_s)

    assert got.key_sha256 != want.key_sha256, "credentials must be usable, so replaced"
    assert set(keys) == {"a"} and keys["a"].startswith("sk_sy_a_")
    assert admin.startswith("sk_sy_admin_")
    derived.validate()


def test_derived_config_round_trips_through_toml(tmp_path):
    derived, _, _ = derive_config(config())
    path = tmp_path / "d.toml"
    path.write_text(render_toml(derived))
    assert load_config(path) == derived


# -- output ----------------------------------------------------------------


def test_advice_is_shown_for_failures_and_skips_but_not_passes():
    """A recommendation under a PASS is noise that buries the real findings."""
    from switchyard.scenarios.base import Check

    buffer = io.StringIO()
    reporter = Reporter(style=PLAIN, stream=buffer)
    reporter.check(Check("fine", Outcome.PASS, "ok", recommendation="do not show"))
    reporter.check(Check("broken", Outcome.FAIL, "bad", "why", "show this"))
    reporter.check(Check.skip("absent", "not configured", "configure it"))

    out = buffer.getvalue()
    assert "do not show" not in out
    assert "-> show this" in out and "why" in out
    assert "-> configure it" in out


# -- end to end ------------------------------------------------------------


@pytest.mark.slow
async def test_verify_passes_against_the_projects_own_configuration():
    from switchyard import verify

    buffer = io.StringIO()
    reporter = Reporter(style=PLAIN, stream=buffer)
    result = await verify.run(reporter, "switchyard.toml", contention_s=6.0, failure_s=6.0)

    assert result.exit_code == 0, [c.name for c in result.checks if not c.passed]
    assert len(result.checks) >= 10
    assert all(c.detail for c in result.checks)
    assert "your real providers are not called" in buffer.getvalue()


@pytest.mark.slow
async def test_verify_fails_a_configuration_that_abandons_work_at_shutdown(tmp_path):
    """A deliberately broken configuration must actually be caught."""
    from dataclasses import replace

    from switchyard import verify

    good = load_config("switchyard.toml")
    broken = replace(good, drain_timeout_s=0.0, admin_key_sha256=None)
    path = tmp_path / "broken.toml"
    path.write_text(render_toml(broken))

    reporter = Reporter(style=PLAIN, stream=io.StringIO())
    result = await verify.run(reporter, str(path), contention_s=5.0, failure_s=5.0)

    assert result.exit_code == 1
    failed = {c.name for c in result.checks if c.outcome is Outcome.FAIL}
    assert "operational endpoints are protected" in failed
    assert "shutdown finishes running work" in failed


# -- additional static checks ----------------------------------------------


def test_a_floor_above_its_own_ceiling_is_reported():
    """The tenant could never reach the guarantee its configuration promises."""
    cfg = GatewayConfig(
        max_concurrency=20, admin_key_sha256=DIGEST,
        tenants=(tenant("a", reserved_concurrency=8, max_concurrency=4),),
    )
    starved = next(c for c in review(cfg) if "floors fit" in c.name)
    assert starved.outcome is Outcome.FAIL
    assert "floor 8 > ceiling 4" in starved.detail


def test_a_zero_budget_tenant_is_reported():
    cfg = config(tenants=(tenant("a", reserved_concurrency=2, budget_tokens=0),))
    zero = next(c for c in review(cfg) if "at least one request" in c.name)
    assert zero.outcome is Outcome.FAIL
    assert "can never be served" in zero.explain


def test_a_route_naming_an_unconfigured_provider_is_rejected_at_load():
    """Handled by validation rather than review: it cannot even load."""
    from switchyard.core.config import ConfigError

    with pytest.raises(ConfigError, match="ghost"):
        config(providers=("p1",), routes={"m": ("p1", "ghost")})


# -- interpretation --------------------------------------------------------


def subjects(items) -> dict[str, str]:
    return {i.subject.strip(): i.meaning for i in items}


def test_interpretation_states_capacity_split_and_weight_share():
    items = subjects(interpret(config()))
    assert "6 reserved as floors, 6 shared" in items["capacity"]   # floors 4 + 2 of 12
    assert "50% of contended capacity" in items["tenant 'a'"]      # equal weights


def test_interpretation_converts_a_floor_into_a_sustainable_rate():
    """Six slots is not a number anyone can act on; requests per second is."""
    items = subjects(interpret(config(), service_s=2.0))
    assert "sustains about 2.0 req/s" in items["tenant 'a'"]      # floor 4 / 2.0s


def test_interpretation_converts_a_budget_into_a_request_count():
    cfg = config(tenants=(tenant("a", reserved_concurrency=2, budget_tokens=10_000),))
    items = subjects(interpret(cfg, service_s=2.0, output_tokens=100.0))
    assert "about 100 more requests" in items["budget"]


def test_interpretation_declines_to_guess_without_measurements():
    """No service time means no honest statement about sustainable rate."""
    cfg = config(tenants=(tenant("a", reserved_concurrency=2, budget_tokens=10_000),))
    items = subjects(interpret(cfg))
    assert "sustains" not in items["tenant 'a'"]
    caveats = [i.caveat for i in interpret(cfg) if i.subject.strip() == "budget"]
    assert "not enough data" in caveats[0]


def test_interpretation_flags_a_model_with_no_fallback():
    items = interpret(config(providers=("p1", "p2"), routes={"m": ("p1",)}))
    model = next(i for i in items if i.subject == "model 'm'")
    assert "only p1" in model.meaning
    assert "reaches the client" in model.caveat


def test_interpretation_states_breaker_trip_threshold():
    cfg = config()
    items = subjects(interpret(cfg))
    assert f"about {breaker_failures_needed(cfg)} failures" in items["circuit breaker"]


def test_interpretation_warns_about_a_short_drain_timeout():
    items = interpret(config(drain_timeout_s=1.0))
    drain = next(i for i in items if i.subject == "shutdown")
    assert "abandoned" in drain.caveat


def test_a_tenant_without_a_floor_is_told_what_that_costs():
    items = interpret(config(tenants=(tenant("a"), tenant("b", reserved_concurrency=2))))
    a = next(i for i in items if i.subject == "tenant 'a'")
    assert "queues behind a busy neighbour" in a.caveat

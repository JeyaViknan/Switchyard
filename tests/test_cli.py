"""CLI parsing, rendering and command wiring.

The rendering functions are pure, so they are tested directly rather than by
scraping terminal output. What matters is that the numbers shown are the
gateway's numbers, and that a broken or unauthorised gateway produces a message
that says what to do rather than a traceback.
"""

from __future__ import annotations

import pytest

from switchyard.cli.main import build_parser, main
from switchyard.cli.render import Style, bar, compact
from switchyard.cli.top import Snapshot, breakdown, parse_metrics, render, total

PLAIN = Style(enabled=False)

METRICS = """\
# HELP switchyard_failovers_total Transparent failovers.
# TYPE switchyard_failovers_total counter
switchyard_failovers_total{from_provider="fast",to_provider="slow"} 26.0
switchyard_admission_rejected_total{reason="queue_full",tenant="acme"} 900.0
switchyard_admission_rejected_total{reason="deadline",tenant="acme"} 85.0
switchyard_tenant_tokens_total{tenant="acme"} 34201.0
switchyard_tenant_tokens_total{tenant="globex"} 8110.0
"""

STATS = {
    "policy": "drr", "max_concurrency": 24, "inflight": 7, "queue_depth": 12,
    "shared_pool": {"in_use": 3, "capacity": 12},
    "tenants": {
        "acme": {"weight": 3.0, "reserved_concurrency": 6, "max_concurrency": 18,
                 "inflight": 5, "queued": 12,
                 "budget": {"limit": 2_000_000, "spent": 1_900_000,
                            "reserved_in_flight": 0, "available": 100_000}},
        "globex": {"weight": 1.0, "reserved_concurrency": 4, "max_concurrency": 12,
                   "inflight": 2, "queued": 0, "budget": {"limit": None}},
    },
}

PROVIDERS = {
    "fast": {"state": "closed", "successes": 1204, "failures": 16,
             "ttft_ewma_ms": 231.0, "reopens_in_s": None, "errors": {}},
    "slow": {"state": "open", "successes": 88, "failures": 104,
             "ttft_ewma_ms": None, "reopens_in_s": 3.2,
             "errors": {"server_error": 104}},
}


def snapshot() -> Snapshot:
    return Snapshot(STATS, PROVIDERS, parse_metrics(METRICS))


# -- metric parsing --------------------------------------------------------


def test_parses_labelled_samples():
    metrics = parse_metrics(METRICS)
    assert total(metrics, "switchyard_failovers_total") == 26.0
    assert total(metrics, "switchyard_tenant_tokens_total", tenant="acme") == 34201.0


def test_comments_and_malformed_lines_are_ignored():
    metrics = parse_metrics("# HELP x\nnot a metric\nswitchyard_x 1.0\n")
    assert total(metrics, "switchyard_x") == 1.0


def test_breakdown_groups_and_sorts_by_size():
    metrics = parse_metrics(METRICS)
    assert list(breakdown(metrics, "switchyard_admission_rejected_total", "reason")) == [
        "queue_full", "deadline"
    ]


def test_missing_metrics_are_zero_not_an_error():
    assert total(parse_metrics(""), "switchyard_failovers_total") == 0.0
    assert breakdown(parse_metrics(""), "switchyard_x", "reason") == {}


# -- rendering -------------------------------------------------------------


def test_render_shows_capacity_queues_and_providers():
    out = render(snapshot(), "http://gw", PLAIN)
    assert "7/24" in out and "queued 12" in out
    assert "acme" in out and "globex" in out
    assert "fast" in out and "closed" in out
    assert "slow" in out and "open" in out


def test_render_surfaces_per_tenant_backlog():
    """The number that makes fair scheduling legible while it happens."""
    out = render(snapshot(), "http://gw", PLAIN)
    acme = next(ln for ln in out.splitlines() if ln.strip().startswith("acme"))
    assert "12" in acme and "5" in acme


def test_render_shows_budget_headroom_and_unlimited():
    out = render(snapshot(), "http://gw", PLAIN)
    assert "100.0K / 2.0M left" in out
    assert "unlimited" in out


def test_render_explains_an_open_breaker():
    out = render(snapshot(), "http://gw", PLAIN)
    assert "retries in 3.2s" in out


def test_render_footer_counts_failovers_and_rejections():
    out = render(snapshot(), "http://gw", PLAIN)
    assert "failovers 26" in out
    assert "rejected 985" in out and "queue_full 900" in out


def test_an_unreachable_gateway_explains_itself_rather_than_crashing():
    out = render(Snapshot({}, {}, error="cannot reach http://gw: refused"), "http://gw", PLAIN)
    assert "cannot reach" in out


def test_colour_is_suppressed_when_disabled():
    assert "\033[" not in render(snapshot(), "http://gw", PLAIN)


# -- formatting helpers ----------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, "0"), (999, "999"), (1500, "1.5K"), (2_000_000, "2.0M"), (3.2e9, "3.2B")],
)
def test_compact_numbers(value, expected):
    assert compact(value) == expected


def test_utilisation_bar_scales_and_clamps():
    assert bar(0.0, width=10, style=PLAIN) == "." * 10
    assert bar(1.0, width=10, style=PLAIN) == "#" * 10
    assert bar(2.0, width=10, style=PLAIN) == "#" * 10
    assert bar(-1.0, width=10, style=PLAIN) == "." * 10


# -- command wiring --------------------------------------------------------


def test_parser_exposes_the_product_commands():
    parser = build_parser()
    for argv in (["serve"], ["top"], ["check"], ["keys", "mint", "t1"]):
        assert parser.parse_args(argv).func is not None


def test_check_reports_an_unloadable_config_without_a_traceback(tmp_path, capsys):
    """Exit 2 for a config that cannot load, distinct from 1 for one that loads badly."""
    bad = tmp_path / "bad.toml"
    bad.write_text("[gateway]\nmax_concurrency = 0\n")
    assert main(["check", "--config", str(bad)]) == 2
    assert "not valid" in capsys.readouterr().err


def test_check_passes_and_explains_a_valid_config(capsys):
    assert main(["check", "--config", "switchyard.toml"]) == 0
    out = capsys.readouterr().out
    assert "Configuration is valid." in out
    assert "PASS" in out and "tenant 'acme'" in out
    assert "falls back to slow" in out, "it should say what the routes actually do"
    assert "switchyard verify" in out, "and point at the next step"


def test_check_fails_when_operational_endpoints_would_be_disabled(tmp_path, capsys):
    """A config that loads but leaves metrics and drain unreachable is a problem."""
    config = tmp_path / "c.toml"
    config.write_text(
        '[gateway]\nmax_concurrency = 4\n\n[[tenants]]\nid = "t1"\n'
        f'key_sha256 = "{"a" * 64}"\n'
    )
    assert main(["check", "--config", str(config)]) == 1
    out = capsys.readouterr().out
    assert "FAIL" in out and "admin_key_sha256" in out
    assert "problem(s) to fix" in out


def test_check_needs_no_running_services(capsys):
    """It is the command you run constantly, so it must not start anything."""
    import time

    started = time.perf_counter()
    main(["check", "--config", "switchyard.toml"])
    capsys.readouterr()
    assert time.perf_counter() - started < 1.0


def test_keys_mint_prints_a_key_and_its_digest(capsys):
    assert main(["keys", "mint", "acme"]) == 0
    out = capsys.readouterr().out
    assert "sk_sy_acme_" in out and "key_sha256" in out


def test_keys_mint_admin_requires_no_tenant_id(capsys):
    assert main(["keys", "mint", "--admin"]) == 0
    out = capsys.readouterr().out
    assert "sk_sy_admin_" in out and "admin_key_sha256" in out


def test_keys_mint_without_a_tenant_id_is_an_error(capsys):
    assert main(["keys", "mint"]) == 2
    assert "tenant id" in capsys.readouterr().err

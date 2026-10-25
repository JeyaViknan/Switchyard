"""Console building blocks: metrics parsing, settings, session auth.

These are pure functions over data, so they are tested without starting
anything. The behaviour that matters most is what happens at the edges --
a counter that reset, a window with no observations, a forged cookie.
"""

from __future__ import annotations

import pytest

from switchyard.console import auth, promtext
from switchyard.console.settings import ConsoleConfigError, load_settings

# -- parsing ---------------------------------------------------------------

SAMPLE = """\
# HELP switchyard_requests_total Requests.
# TYPE switchyard_requests_total counter
switchyard_requests_total{model="fast",provider="fast",outcome="ok"} 100.0
switchyard_requests_total{model="fast",provider="fast",outcome="error"} 5.0
switchyard_requests_total{model="fast",provider="slow",outcome="ok"} 20.0
# HELP switchyard_breaker_state Breaker.
# TYPE switchyard_breaker_state gauge
switchyard_breaker_state{provider="fast"} 2.0
# HELP switchyard_queue_wait_seconds Queue wait.
# TYPE switchyard_queue_wait_seconds histogram
switchyard_queue_wait_seconds_bucket{tenant="acme",le="0.1"} 10.0
switchyard_queue_wait_seconds_bucket{tenant="acme",le="1.0"} 18.0
switchyard_queue_wait_seconds_bucket{tenant="acme",le="+Inf"} 20.0
switchyard_queue_wait_seconds_count{tenant="acme"} 20.0
switchyard_queue_wait_seconds_sum{tenant="acme"} 7.5
"""


def test_counters_gauges_and_histograms_are_separated():
    s = promtext.parse(SAMPLE, at=100.0)
    assert s.counter("switchyard_requests_total", model="fast", provider="fast",
                     outcome="ok") == 100.0
    assert s.gauge("switchyard_breaker_state", provider="fast") == 2.0
    hist = s.histograms["switchyard_queue_wait_seconds"][promtext.labels_of({"tenant": "acme"})]
    assert hist.count == 20.0
    assert hist.buckets[0] == (0.1, 10.0)


def test_counter_total_matches_a_label_subset():
    s = promtext.parse(SAMPLE, at=100.0)
    assert s.counter_total("switchyard_requests_total", outcome="ok") == 120.0
    assert s.counter_total("switchyard_requests_total", provider="fast") == 105.0
    assert s.counter_total("switchyard_requests_total") == 125.0


def test_label_values_enumerates_series():
    s = promtext.parse(SAMPLE, at=100.0)
    assert s.label_values("switchyard_requests_total", "provider") == {"fast", "slow"}


def test_missing_series_reads_zero_not_an_error():
    """A counter has no series until first increment; absence means none."""
    s = promtext.parse(SAMPLE, at=100.0)
    assert s.counter("switchyard_requests_total", provider="nope", model="x",
                     outcome="ok") == 0.0
    assert s.gauge("switchyard_breaker_state", provider="nope") is None


# -- rates -----------------------------------------------------------------


def test_rate_is_per_second_over_the_gap():
    a = promtext.parse(SAMPLE, at=100.0)
    b = promtext.parse(SAMPLE.replace("outcome=\"ok\"} 100.0", "outcome=\"ok\"} 110.0"), at=102.0)
    r = promtext.rate(a, b, "switchyard_requests_total", model="fast",
                      provider="fast", outcome="ok")
    assert r == pytest.approx(5.0)


def test_first_scrape_has_no_rate():
    """One sample is a total, not a rate. Reporting 0 beats inventing one."""
    b = promtext.parse(SAMPLE, at=100.0)
    assert promtext.rate(None, b, "switchyard_requests_total", outcome="ok") == 0.0


def test_a_counter_reset_is_not_a_negative_rate():
    """The gateway restarted: totals went back to zero and climbed again."""
    before = promtext.parse(SAMPLE, at=100.0)
    after = promtext.parse(SAMPLE.replace('outcome="ok"} 100.0', 'outcome="ok"} 3.0'), at=101.0)
    r = promtext.rate(before, after, "switchyard_requests_total",
                      model="fast", provider="fast", outcome="ok")
    assert r == pytest.approx(3.0), "post-reset value is the whole delta, never negative"


def test_zero_or_backwards_elapsed_time_yields_no_rate():
    a = promtext.parse(SAMPLE, at=100.0)
    b = promtext.parse(SAMPLE, at=100.0)
    assert promtext.rate(a, b, "switchyard_requests_total", outcome="ok") == 0.0


def test_delta_handles_both_directions():
    assert promtext.delta(10.0, 25.0) == 15.0
    assert promtext.delta(10.0, 3.0) == 3.0


# -- quantiles -------------------------------------------------------------


def hist(pairs, count):
    return promtext.Histogram(buckets=tuple(pairs), count=count, sum=0.0)


def test_quantile_interpolates_inside_the_bucket():
    current = hist([(0.1, 10.0), (1.0, 18.0), (float("inf"), 20.0)], 20.0)
    q = promtext.quantile_from_buckets(None, current, q=0.5)
    assert q is not None
    assert 0.1 <= q <= 1.0


def test_quantile_is_none_when_the_window_saw_nothing():
    """No observations means no percentile. 0 would read as 'very fast'."""
    same = hist([(0.1, 10.0), (float("inf"), 10.0)], 10.0)
    assert promtext.quantile_from_buckets(same, same) is None


def test_quantile_describes_the_window_not_all_time():
    """Old observations in the slow bucket must not drag the window's p95."""
    before = hist([(0.1, 0.0), (1.0, 0.0), (float("inf"), 100.0)], 100.0)
    after = hist([(0.1, 50.0), (1.0, 50.0), (float("inf"), 150.0)], 150.0)
    q = promtext.quantile_from_buckets(before, after, q=0.95)
    assert q is not None
    assert 0.0 < q <= 0.1, "everything new landed in the fast bucket"


def test_quantile_survives_a_histogram_reset():
    before = hist([(0.1, 90.0), (float("inf"), 100.0)], 100.0)
    after = hist([(0.1, 4.0), (float("inf"), 5.0)], 5.0)
    assert promtext.quantile_from_buckets(before, after, q=0.5) is not None


# -- settings --------------------------------------------------------------


def test_defaults_work_with_no_environment(monkeypatch):
    for name in ("CONSOLE_GATEWAY_URL", "CONSOLE_DEMO_KEYS", "CONSOLE_PASSWORD",
                 "SWITCHYARD_ADMIN_KEY", "CONSOLE_DEMO_CONTROLS", "SWITCHYARD_GIT_SHA"):
        monkeypatch.delenv(name, raising=False)
    s = load_settings()
    assert s.gateway_url == "http://127.0.0.1:8000"
    assert s.git_sha == "dev"
    assert s.demo_controls_available is False, "no demo keys means no controls"


def test_demo_keys_parse_from_json(monkeypatch):
    monkeypatch.setenv("CONSOLE_DEMO_KEYS", '{"acme": "sk_sy_acme_x", "globex": "sk_sy_g_y"}')
    monkeypatch.setenv("CONSOLE_PASSWORD", "hunter2")
    s = load_settings()
    assert s.demo_keys["acme"] == "sk_sy_acme_x"
    assert s.demo_controls_available is True


def test_malformed_demo_keys_say_what_was_expected(monkeypatch):
    monkeypatch.setenv("CONSOLE_DEMO_KEYS", "not json")
    with pytest.raises(ConsoleConfigError, match="valid JSON"):
        load_settings()


def test_controls_without_a_password_are_refused(monkeypatch):
    """Open traffic generation and fault injection is the one fatal config."""
    monkeypatch.setenv("CONSOLE_DEMO_KEYS", '{"acme": "sk_sy_acme_x"}')
    monkeypatch.delenv("CONSOLE_PASSWORD", raising=False)
    with pytest.raises(ConsoleConfigError, match="CONSOLE_PASSWORD"):
        load_settings()


def test_controls_can_be_turned_off_for_real_providers(monkeypatch):
    """Section 9: a company running live providers disables the demo controls."""
    monkeypatch.setenv("CONSOLE_DEMO_KEYS", '{"acme": "sk_sy_acme_x"}')
    monkeypatch.delenv("CONSOLE_PASSWORD", raising=False)
    monkeypatch.setenv("CONSOLE_DEMO_CONTROLS", "false")
    s = load_settings()
    assert s.demo_controls_available is False


def test_redacted_settings_never_carry_values(monkeypatch):
    monkeypatch.setenv("SWITCHYARD_ADMIN_KEY", "sk_sy_admin_supersecret")
    monkeypatch.setenv("CONSOLE_DEMO_KEYS", '{"acme": "sk_sy_acme_secret"}')
    monkeypatch.setenv("CONSOLE_PASSWORD", "hunter2")
    text = repr(load_settings().redacted())
    assert "supersecret" not in text
    assert "sk_sy_acme_secret" not in text
    assert "hunter2" not in text
    assert "acme" in text, "tenant names are not secret and are useful"


# -- session auth ----------------------------------------------------------


def test_a_fresh_token_verifies():
    token = auth.issue("secret", now=1000.0)
    assert auth.verify("secret", token, now=1001.0)


def test_an_expired_token_does_not():
    token = auth.issue("secret", now=1000.0, ttl_s=10)
    assert not auth.verify("secret", token, now=1011.0)


def test_a_token_signed_with_another_secret_does_not():
    token = auth.issue("secret", now=1000.0)
    assert not auth.verify("different", token, now=1001.0)


@pytest.mark.parametrize("tampered", [
    "9999999999.badsignature", "9999999999", "", "...", "notanumber.sig",
])
def test_malformed_and_tampered_tokens_are_refused(tampered):
    assert not auth.verify("secret", tampered, now=1000.0)


def test_extending_the_expiry_invalidates_the_signature():
    """The payload is the expiry, so editing it is what the HMAC prevents."""
    token = auth.issue("secret", now=1000.0, ttl_s=10)
    _, _, signature = token.partition(".")
    assert not auth.verify("secret", f"9999999999.{signature}", now=1011.0)


def test_no_secret_means_no_valid_session():
    assert not auth.verify("", auth.issue("", now=1000.0), now=1000.1)


def test_unset_password_matches_nothing():
    assert not auth.password_matches("", "")
    assert not auth.password_matches("", "guess")


def test_password_comparison():
    assert auth.password_matches("hunter2", "hunter2")
    assert not auth.password_matches("hunter2", "hunter3")


def test_login_attempts_are_limited_per_client():
    limiter = auth.AttemptLimiter(max_attempts=3, window_s=60.0)
    assert [limiter.allow("1.2.3.4", now=float(i)) for i in range(4)] == [
        True, True, True, False]
    # A different client is unaffected.
    assert limiter.allow("5.6.7.8", now=3.0)


def test_the_limit_window_slides():
    limiter = auth.AttemptLimiter(max_attempts=2, window_s=60.0)
    limiter.allow("ip", now=0.0)
    limiter.allow("ip", now=1.0)
    assert not limiter.allow("ip", now=30.0)
    assert limiter.allow("ip", now=61.5), "the first attempt has aged out"


def test_a_successful_login_clears_the_throttle():
    limiter = auth.AttemptLimiter(max_attempts=2, window_s=60.0)
    limiter.allow("ip", now=0.0)
    limiter.allow("ip", now=1.0)
    limiter.reset("ip")
    assert limiter.allow("ip", now=2.0)

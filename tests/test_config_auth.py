"""Configuration and authentication tests.

Configuration errors are worth testing because a misconfigured gateway fails
under load rather than at startup, which is the worst time to find out. Every
case below is one that would otherwise surface as a capacity problem in
production.
"""

from __future__ import annotations

import pytest

from switchyard.core.auth import (
    AuthError,
    TenantRegistry,
    bearer_from_header,
    hash_key,
    mint_key,
    tenant_id_from_key,
)
from switchyard.core.config import ConfigError, GatewayConfig, Tenant, load_config

VALID_DIGEST = "a" * 64


def tenant(**over) -> Tenant:
    return Tenant(**({"id": "t1", "key_sha256": VALID_DIGEST} | over))


def write(tmp_path, body: str):
    path = tmp_path / "switchyard.toml"
    path.write_text(body)
    return path


# -- configuration ---------------------------------------------------------


def test_loads_the_committed_development_config():
    config = load_config("switchyard.toml")
    assert config.max_concurrency > 0
    assert {t.id for t in config.tenants} == {"acme", "globex", "initech"}
    assert config.scheduling_policy in ("drr", "fifo")


def test_missing_config_file_names_the_path(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.toml")


def test_unknown_tenant_field_is_rejected_by_name(tmp_path):
    path = write(tmp_path, f'''
[[tenants]]
id = "t1"
key_sha256 = "{VALID_DIGEST}"
weght = 3.0
''')
    with pytest.raises(ConfigError, match="weght"):
        load_config(path)


def test_unknown_gateway_field_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="max_concurency"):
        load_config(write(tmp_path, "[gateway]\nmax_concurency = 4\n"))


def test_duplicate_tenant_ids_are_rejected(tmp_path):
    path = write(tmp_path, f'''
[[tenants]]
id = "t1"
key_sha256 = "{VALID_DIGEST}"
[[tenants]]
id = "t1"
key_sha256 = "{VALID_DIGEST}"
''')
    with pytest.raises(ConfigError, match="duplicate"):
        load_config(path)


def test_reserved_capacity_beyond_the_total_is_rejected():
    """Guarantees that cannot all be honoured at once are an overdraft, not a floor."""
    config = GatewayConfig(
        max_concurrency=4,
        tenants=(tenant(id="a", reserved_concurrency=3), tenant(id="b", reserved_concurrency=3)),
    )
    with pytest.raises(ConfigError, match="cannot all be honoured"):
        config.validate()


def test_reserved_above_a_tenants_own_ceiling_is_rejected():
    config = GatewayConfig(tenants=(tenant(reserved_concurrency=8, max_concurrency=4),))
    with pytest.raises(ConfigError, match="never reach its own guarantee"):
        config.validate()


@pytest.mark.parametrize(
    ("over", "fragment"),
    [
        ({"weight": 0}, "weight"),
        ({"weight": -1}, "weight"),
        ({"key_sha256": "short"}, "64-char"),
        ({"budget_tokens": -5}, "budget_tokens"),
        ({"deadline_s": 0}, "deadline_s"),
        ({"max_concurrency": 0}, "max_concurrency"),
        ({"id": "bad id!"}, "alphanumeric"),
    ],
)
def test_invalid_tenant_fields_are_rejected(over, fragment):
    with pytest.raises(ConfigError, match=fragment):
        GatewayConfig(tenants=(tenant(**over),)).validate()


def test_unknown_scheduling_policy_is_rejected():
    with pytest.raises(ConfigError, match="scheduling_policy"):
        GatewayConfig(scheduling_policy="magic").validate()


# -- authentication --------------------------------------------------------


def registry(*tenants: Tenant) -> TenantRegistry:
    return TenantRegistry(tenants={t.id: t for t in tenants})


def test_minted_key_authenticates():
    raw, digest = mint_key("acme")
    reg = registry(tenant(id="acme", key_sha256=digest))
    assert reg.authenticate(raw).id == "acme"


def test_key_carries_its_tenant_id():
    raw, _ = mint_key("globex")
    assert tenant_id_from_key(raw) == "globex"
    assert tenant_id_from_key("nonsense") is None


@pytest.mark.parametrize(
    "key",
    [None, "", "garbage", "sk_sy_acme_wrongsecret", "sk_sy_unknown_abc123", "Bearer x"],
)
def test_every_authentication_failure_gives_the_same_message(key):
    """Distinct errors would let an unauthenticated caller enumerate tenant ids."""
    raw, digest = mint_key("acme")
    reg = registry(tenant(id="acme", key_sha256=digest))
    with pytest.raises(AuthError) as exc:
        reg.authenticate(key)
    assert str(exc.value) in ("missing API key", "invalid API key")


def test_a_valid_key_for_one_tenant_does_not_authenticate_another():
    raw_a, digest_a = mint_key("acme")
    _, digest_b = mint_key("globex")
    reg = registry(
        tenant(id="acme", key_sha256=digest_a), tenant(id="globex", key_sha256=digest_b)
    )
    assert reg.authenticate(raw_a).id == "acme"

    # A key whose embedded id is swapped must not authenticate as the other
    # tenant: the id in the key is a claim, and the digest is what decides.
    forged = raw_a.replace("acme", "globex", 1)
    with pytest.raises(AuthError):
        reg.authenticate(forged)


def test_pepper_changes_the_digest():
    """A leaked config file is not enough to mint a working key."""
    assert hash_key("sk_sy_a_b", with_pepper="one") != hash_key("sk_sy_a_b", with_pepper="two")


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("Bearer sk_sy_a_b", "sk_sy_a_b"),
        ("bearer sk_sy_a_b", "sk_sy_a_b"),
        ("sk_sy_a_b", "sk_sy_a_b"),
        (None, None),
        ("", None),
    ],
)
def test_bearer_header_parsing(header, expected):
    assert bearer_from_header(header) == expected

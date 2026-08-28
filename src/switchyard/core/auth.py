"""API key handling and tenant resolution.

Key format is `sk_sy_<tenant-id>_<32 hex>`. The tenant id is carried in the key
itself, so resolution is a dict lookup rather than a scan over every configured
tenant's digest.

Hashing is SHA-256 with a server-side pepper, not argon2 or bcrypt. Password
KDFs are deliberately slow to make brute force expensive against *low-entropy*
secrets; a 128-bit random key has nothing to brute-force, and spending ~50ms of
CPU per request to protect it would make authentication the largest single
component of gateway latency. The pepper defends against a leaked config file
being used directly, since the digests alone are not enough to mint a key.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass

from switchyard.core.config import GatewayConfig, Tenant

KEY_PREFIX = "sk_sy"
PEPPER_ENV = "SWITCHYARD_KEY_PEPPER"

# Used when no pepper is configured. Fine for local development, where the
# threat model is nobody; a deployment that cares sets the environment variable.
DEV_PEPPER = "switchyard-dev-pepper"


def pepper() -> str:
    return os.environ.get(PEPPER_ENV, DEV_PEPPER)


def hash_key(raw_key: str, *, with_pepper: str | None = None) -> str:
    return hashlib.sha256(f"{raw_key}{with_pepper or pepper()}".encode()).hexdigest()


def mint_admin_key() -> tuple[str, str]:
    """Generate an operational credential. Returns (raw_key, sha256_digest)."""
    return mint_key(ADMIN_ID)


def mint_key(tenant_id: str) -> tuple[str, str]:
    """Generate a key for a tenant. Returns (raw_key, sha256_digest).

    The raw key is shown once and never stored; only the digest goes in config.
    """
    raw = f"{KEY_PREFIX}_{tenant_id}_{secrets.token_hex(16)}"
    return raw, hash_key(raw)


def tenant_id_from_key(raw_key: str) -> str | None:
    """Extract the claimed tenant id. Claimed, not verified -- the digest decides."""
    parts = raw_key.split("_")
    if len(parts) < 4 or f"{parts[0]}_{parts[1]}" != KEY_PREFIX:
        return None
    return "_".join(parts[2:-1]) or None


ADMIN_ID = "admin"


class AdminAuthError(Exception):
    """Operational endpoint access was refused."""


def authenticate_admin(raw_key: str | None, expected_sha256: str | None,
                       *, required: bool) -> None:
    """Guard the operational endpoints.

    When no tenants are configured the gateway is in open development mode and
    these are open too, so a fresh checkout works without setup. As soon as real
    tenants exist the endpoints are protected, and a deployment that configured
    tenants but no admin key is refused rather than silently left open -- an
    endpoint that can drain the gateway should fail closed.
    """
    if not required:
        return
    if not expected_sha256:
        raise AdminAuthError(
            "operational endpoints are disabled: set gateway.admin_key_sha256 "
            "in your config (mint one with `switchyard keys mint --admin`)"
        )
    if not raw_key or not hmac.compare_digest(hash_key(raw_key), expected_sha256):
        raise AdminAuthError("invalid admin key")


class AuthError(Exception):
    """Authentication failed. The message is deliberately non-specific."""


@dataclass(slots=True)
class TenantRegistry:
    """Resolves API keys to tenants."""

    tenants: dict[str, Tenant]

    @classmethod
    def from_config(cls, config: GatewayConfig) -> TenantRegistry:
        return cls(tenants=config.tenants_by_id)

    def get(self, tenant_id: str) -> Tenant | None:
        return self.tenants.get(tenant_id)

    def authenticate(self, raw_key: str | None) -> Tenant:
        """Resolve a key to a tenant, or raise AuthError.

        Every failure raises the same message. Distinguishing "no such tenant"
        from "wrong key" would let an unauthenticated caller enumerate which
        tenant ids exist.
        """
        if not raw_key:
            raise AuthError("missing API key")

        claimed = tenant_id_from_key(raw_key)
        tenant = self.tenants.get(claimed) if claimed else None
        if tenant is None:
            # Still hash, so a bad tenant id and a bad secret take the same time.
            hash_key(raw_key)
            raise AuthError("invalid API key")

        if not hmac.compare_digest(hash_key(raw_key), tenant.key_sha256):
            raise AuthError("invalid API key")
        return tenant


def bearer_from_header(header: str | None) -> str | None:
    """Extract a key from an Authorization header, tolerating a bare key."""
    if not header:
        return None
    scheme, _, value = header.partition(" ")
    if scheme.lower() == "bearer" and value:
        return value.strip()
    return header.strip() or None


def main(argv: list[str] | None = None) -> int:
    """Mint an API key: `python -m switchyard.core.auth <tenant-id>`."""
    import argparse

    parser = argparse.ArgumentParser(description="Mint a Switchyard API key")
    parser.add_argument("tenant_id")
    args = parser.parse_args(argv)

    raw, digest = mint_key(args.tenant_id)
    print(f"key    {raw}")
    print(f"digest {digest}")
    print("\nAdd to switchyard.toml:\n")
    print("[[tenants]]")
    print(f'id = "{args.tenant_id}"')
    print(f'key_sha256 = "{digest}"')
    print("\nThe key is shown once. Only the digest is stored.")
    if PEPPER_ENV not in os.environ:
        print(f"\nNote: {PEPPER_ENV} is unset, so the development pepper was used.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

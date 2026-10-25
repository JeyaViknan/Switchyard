"""Session auth for the console's demo controls.

Scope, stated plainly: this protects buttons that generate load against the
local gateway and inject faults into the synthetic fleet. It is not an
identity system. There is one password and one role, because there is one
operator -- adding users would be more surface to get wrong for no gain.

The cookie is an HMAC-signed expiry, nothing more. There is no session store,
so there is no state to grow, expire or lose across a restart; the cost is
that a session cannot be revoked before it expires, which for a 12-hour demo
credential is the right trade. The secret is read from the environment and
regenerated per deployment, so redeploying invalidates every outstanding
cookie anyway.

What this cannot do is be `Secure`. The free-tier deployment is plain HTTP --
no load balancer, no certificate, no budget for either -- so the cookie would
never be sent if it were marked Secure. That is a real weakness and it is
recorded in `docs/decisions.md` rather than hidden: anyone on the path can
read the cookie and drive the demo controls. HTTPS is the first thing to add
when there is a budget.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from collections import deque
from dataclasses import dataclass, field

COOKIE_NAME = "switchyard_console"
SESSION_TTL_S = 12 * 60 * 60

# A POST must carry this header. Browsers will not set a custom header on a
# cross-origin form post without a successful preflight, so it costs one line
# and removes the simplest CSRF: a hostile page submitting a form at the
# console while an operator happens to be logged in.
CSRF_HEADER = "x-console"
CSRF_VALUE = "1"

MAX_ATTEMPTS = 5
ATTEMPT_WINDOW_S = 60.0


def _sign(secret: str, payload: str) -> str:
    digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


def issue(secret: str, now: float | None = None, ttl_s: int = SESSION_TTL_S) -> str:
    """Mint a session token that expires on its own."""
    expires_at = int((now if now is not None else time.time()) + ttl_s)
    payload = str(expires_at)
    return f"{payload}.{_sign(secret, payload)}"


def verify(secret: str, token: str | None, now: float | None = None) -> bool:
    """Is this a token we issued, and is it still valid?

    Signature first, then expiry. Checking expiry first would let an attacker
    learn whether a forged token's payload parsed, which is not much of a leak
    but is free to avoid.
    """
    if not token or not secret:
        return False
    payload, _, signature = token.partition(".")
    if not payload or not signature:
        return False
    # Constant time: a timing-variable compare leaks the signature one byte at
    # a time to anyone willing to make enough requests.
    if not hmac.compare_digest(signature, _sign(secret, payload)):
        return False
    try:
        expires_at = int(payload)
    except ValueError:
        return False
    return (now if now is not None else time.time()) < expires_at


def password_matches(expected: str, supplied: str) -> bool:
    """Constant-time password check. An unset password matches nothing."""
    if not expected:
        return False
    return hmac.compare_digest(expected, supplied)


@dataclass(slots=True)
class AttemptLimiter:
    """Per-client login throttle.

    A single password with no rate limit is a password that gets guessed. The
    window is a deque of timestamps per key rather than a counter, so the limit
    is a true sliding window: five attempts in any sixty seconds, not five per
    fixed minute with a free reset on the boundary.
    """

    max_attempts: int = MAX_ATTEMPTS
    window_s: float = ATTEMPT_WINDOW_S
    _seen: dict[str, deque[float]] = field(default_factory=dict)

    def allow(self, key: str, now: float | None = None) -> bool:
        """Record an attempt from `key`; False means it should be refused."""
        moment = now if now is not None else time.monotonic()
        attempts = self._seen.setdefault(key, deque())
        cutoff = moment - self.window_s
        while attempts and attempts[0] < cutoff:
            attempts.popleft()
        if len(attempts) >= self.max_attempts:
            return False
        attempts.append(moment)
        return True

    def reset(self, key: str) -> None:
        """Forget a client's attempts, called after a successful login."""
        self._seen.pop(key, None)

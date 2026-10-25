#!/usr/bin/env bash
# Prove a running stack actually works, not merely that it started.
#
# Called after `compose up` by the deploy and by `make prod-local`. The check
# that matters is the SHA: a deploy that reports success without confirming
# which commit answered is asking to be trusted.
#
# Usage: smoke.sh [expected-sha] [base-url]
set -euo pipefail

EXPECTED_SHA="${1:-}"
BASE="${2:-http://localhost}"
API="${BASE}:8000"
CONSOLE="${BASE}"
fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }
ok()   { echo "  ok  $*"; }

echo "smoke test against ${API}"

# 1. Liveness, then readiness. Separately, because they answer different
#    questions and a drain makes them disagree.
curl -fsS --max-time 5 "${API}/livez" >/dev/null || fail "/livez did not answer"
ok "/livez"

READY=$(curl -fsS --max-time 5 "${API}/readyz") || fail "/readyz returned non-200"
ok "/readyz"

# 2. The SHA that answered is the SHA we shipped.
if [ -n "${EXPECTED_SHA}" ]; then
  GOT=$(printf '%s' "${READY}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("git_sha",""))')
  case "${EXPECTED_SHA}" in
    "${GOT}"*) ok "serving ${GOT}" ;;
    *) fail "expected sha ${EXPECTED_SHA}, /readyz reports '${GOT}'" ;;
  esac
fi

# 3. A real completion through the whole path: auth, scheduler, budget,
#    adapter, provider, stream. A health endpoint proves none of that.
KEY="${SMOKE_TENANT_KEY:-}"
if [ -n "${KEY}" ]; then
  BODY='{"model":"fast","messages":[{"role":"user","content":"smoke"}],"max_tokens":16,"stream":true}'
  OUT=$(curl -fsS --max-time 30 -N "${API}/v1/chat/completions" \
          -H "Authorization: Bearer ${KEY}" -H 'Content-Type: application/json' \
          -d "${BODY}") || fail "completion request failed"
  printf '%s' "${OUT}" | grep -q 'data:' || fail "no SSE frames in the response"
  # A typed error frame is still a 200, so check how the stream ended rather
  # than trusting the status code -- the load generator made this exact
  # mistake once and reported a total outage as 370 successes.
  printf '%s' "${OUT}" | grep -q '"finish_reason":"stop"' \
    || fail "stream did not finish cleanly: $(printf '%s' "${OUT}" | tail -c 200)"
  ok "completion streamed and finished cleanly"
else
  echo "  --  skipping completion: set SMOKE_TENANT_KEY to exercise it"
fi

# 4. The console is up and talking to the gateway.
SNAP=$(curl -fsS --max-time 5 "${CONSOLE}/api/snapshot") || fail "console /api/snapshot failed"
printf '%s' "${SNAP}" | grep -q '"gateway"' || fail "console snapshot has no gateway section"
ok "console snapshot"

echo "SMOKE PASS"

#!/usr/bin/env bash
# Mint the deployed credentials and put them in SSM Parameter Store.
#
# RUN THIS YOURSELF. It writes to AWS, and nothing in this repository's
# automation is allowed to create or change AWS resources on your behalf.
#
# What it does:
#   1. generates a key pepper, an admin key, and one key per tenant
#   2. writes every raw secret to SSM as a SecureString
#   3. prints the digests to paste into deploy/switchyard.prod.toml
#
# The raw keys are shown once, here, and then exist only in SSM. The digests
# are safe to commit: without the pepper they cannot be reversed or reused.
set -euo pipefail

cd "$(dirname "$0")/.."
REGION="${AWS_REGION:-ap-south-1}"
PREFIX="/switchyard"

command -v aws >/dev/null || { echo "aws cli not found" >&2; exit 1; }
[ -x .venv/bin/python ] || { echo "run 'make install' first" >&2; exit 1; }

echo "Region: ${REGION}"
echo "This writes SecureString parameters under ${PREFIX}/. Continue? [y/N]"
read -r reply
[ "${reply}" = "y" ] || { echo "aborted"; exit 1; }

put() {  # name value
  aws ssm put-parameter --region "${REGION}" --name "$1" --value "$2" \
      --type SecureString --overwrite >/dev/null
  echo "  wrote $1"
}

# The pepper has to exist before any key is hashed against it.
PEPPER="$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))')"
put "${PREFIX}/gateway/SWITCHYARD_KEY_PEPPER" "${PEPPER}"

mint() {  # tenant-or---admin  -> "key digest"
  SWITCHYARD_KEY_PEPPER="${PEPPER}" .venv/bin/switchyard keys mint "$@" \
    | awk '/^key /{k=$2} /^digest /{d=$2} END{print k, d}'
}

read -r ADMIN_KEY ADMIN_DIGEST <<< "$(mint --admin)"
put "${PREFIX}/console/SWITCHYARD_ADMIN_KEY" "${ADMIN_KEY}"

DEMO_KEYS="{"
DIGESTS=""
for tenant in acme globex initech; do
  read -r KEY DIGEST <<< "$(mint "${tenant}")"
  DEMO_KEYS="${DEMO_KEYS}\"${tenant}\":\"${KEY}\","
  DIGESTS="${DIGESTS}${tenant} ${DIGEST}"$'\n'
done
DEMO_KEYS="${DEMO_KEYS%,}}"
put "${PREFIX}/console/CONSOLE_DEMO_KEYS" "${DEMO_KEYS}"

put "${PREFIX}/console/CONSOLE_PASSWORD" \
    "$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(18))')"
put "${PREFIX}/console/CONSOLE_SESSION_SECRET" \
    "$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(32))')"
put "${PREFIX}/grafana/GF_SECURITY_ADMIN_PASSWORD" \
    "$(.venv/bin/python -c 'import secrets; print(secrets.token_urlsafe(18))')"

cat <<MSG

Done. Paste these into deploy/switchyard.prod.toml, replacing the
all-zero placeholders in this order:

  admin_key_sha256 = "${ADMIN_DIGEST}"

$(echo "${DIGESTS}" | sed '/^$/d' | while read -r t d; do echo "  [[tenants]] id = \"${t}\"  ->  key_sha256 = \"${d}\""; done)

Then commit the config. The raw keys are in SSM and are not printed again;
read the console password back with:

  aws ssm get-parameter --region ${REGION} \\
      --name ${PREFIX}/console/CONSOLE_PASSWORD --with-decryption \\
      --query Parameter.Value --output text

Still to set by hand (they are not ours to generate):
  ${PREFIX}/grafana/TELEGRAM_BOT_TOKEN
  ${PREFIX}/grafana/TELEGRAM_CHAT_ID
MSG

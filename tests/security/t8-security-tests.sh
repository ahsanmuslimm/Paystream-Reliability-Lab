#!/usr/bin/env bash
# T8 security tests - negative assertions against the SECURED stack (WP2.2).
# Requires `make up-secure` with SCRAM users registered. Asserts:
#   1. plaintext is refused on secured listeners
#   2. wrong SCRAM credentials are refused
#   3. expired certificates are refused (PKI + handshake level)
#   4. each principal is denied operations outside the ACL matrix (FR-09)
# Usage: tests/security/t8-security-tests.sh [--help] [--only plaintext|auth|cert|acls]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SECRETS="$REPO_ROOT/security/secrets"
CERTS="$REPO_ROOT/security/certs"
HOST_SASL_PORT="${KAFKA_1_HOST_PORT:-19091}"
HOST_MTLS_PORT="${KAFKA_BROKER_HOST_PORT:-19094}"
STORE_PASSWORD="${KAFKA_SSL_PASSWORD:-paystream-lab}"

PASSED=0
FAILED=0
ONLY="all"

usage() {
  cat <<'EOF'
T8 - security negative tests against the secured stack

Options:
  -h, --help                       Show this help.
  --only plaintext|auth|cert|acls  Run a single test group.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --only) ONLY="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

pass() { echo "  PASS: $1"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1"; FAILED=$((FAILED + 1)); }

props_for() {
  # $1 = principal, $2 = password, $3 = port -> prints a client properties file
  cat <<EOF
bootstrap.servers=localhost:${3}
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$1" password="$2";
ssl.truststore.location=${CERTS}/ca/truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=${STORE_PASSWORD}
EOF
}

echo "== T8 security tests ($(date -u +%H:%M:%SZ)) =="

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker not running - T8 requires the secured stack (make up-secure)." >&2
  exit 1
fi

# ---- 1. plaintext refused -----------------------------------------------------
if [ "$ONLY" = "all" ] || [ "$ONLY" = "plaintext" ]; then
  echo "-- plaintext on a secured listener"
  if timeout 15 docker compose --env-file .env -f infra/compose/docker-compose.yml \
      exec -T kafka-1 kafka-topics.sh --bootstrap-server localhost:29092 --list >/dev/null 2>&1; then
    fail "plaintext kafka-topics call SUCCEEDED against the SASL_SSL listener"
  else
    pass "plaintext refused on secured listener"
  fi
fi

# ---- 2. wrong credentials refused ---------------------------------------------
if [ "$ONLY" = "all" ] || [ "$ONLY" = "auth" ]; then
  echo "-- SCRAM authentication"
  [ -f "$SECRETS/scram-credentials.env" ] || { echo "ERROR: run 'make scram' first" >&2; exit 1; }
  GOOD_PW="$(grep '^svc-notify=' "$SECRETS/scram-credentials.env" | cut -d= -f2-)"

  GOOD_PROPS="$(props_for svc-notify "$GOOD_PW" "$HOST_SASL_PORT")"
  if timeout 20 docker compose --env-file .env -f infra/compose/docker-compose.yml \
      exec -T kafka-1 bash -c "kafka-topics.sh --bootstrap-server kafka-1:29092 --command-config /dev/stdin --list" \
      <<<"$GOOD_PROPS" >/dev/null 2>&1; then
    pass "correct credentials authenticate"
  else
    fail "correct credentials were rejected"
  fi

  BAD_PROPS="$(props_for svc-notify "wrong-password-entirely" "$HOST_SASL_PORT")"
  if timeout 20 docker compose --env-file .env -f infra/compose/docker-compose.yml \
      exec -T kafka-1 bash -c "kafka-topics.sh --bootstrap-server kafka-1:29092 --command-config /dev/stdin --list" \
      <<<"$BAD_PROPS" >/dev/null 2>&1; then
    fail "wrong password was ACCEPTED"
  else
    pass "wrong password refused"
  fi
fi

# ---- 3. expired certificate refused --------------------------------------------
if [ "$ONLY" = "all" ] || [ "$ONLY" = "cert" ]; then
  echo "-- certificate expiry (PKI layer)"
  TMPDIR_CERT="$(mktemp -d)"
  bash "$REPO_ROOT/security/scripts/gen-client-cert.sh" --name svc-demo --validity-days 0 >/dev/null 2>&1
  FUTURE=$(( $(date +%s) + 86400 ))
  if openssl verify -CAfile "$CERTS/ca/ca.crt" -attime "$FUTURE" "$CERTS/clients/svc-demo/svc-demo.crt" >/dev/null 2>&1; then
    fail "expired certificate passed openssl verification"
  else
    pass "expired certificate refused by verification"
  fi
  rm -rf "$TMPDIR_CERT"
  bash "$REPO_ROOT/security/scripts/gen-client-cert.sh" --name svc-demo >/dev/null 2>&1
  pass "svc-demo certificate restored to valid"
fi

# ---- 4. ACL negative matrix (FR-09) --------------------------------------------
if [ "$ONLY" = "all" ] || [ "$ONLY" = "acls" ]; then
  echo "-- ACL denials per principal"
  [ -f "$SECRETS/scram-credentials.env" ] || { echo "ERROR: run 'make scram' first" >&2; exit 1; }
  pw_of() { grep "^$1=" "$SECRETS/scram-credentials.env" | cut -d= -f2-; }

  # produce_to(): exit 0 if producing WORKED (unexpected for denials)
  produce_to() {
    local principal="$1" pw="$2" topic="$3"
    local props
    props="$(props_for "$principal" "$pw" "$HOST_SASL_PORT")"
    printf 'acl-negative-test\n' | timeout 20 docker compose --env-file .env \
      -f infra/compose/docker-compose.yml exec -T kafka-1 \
      bash -c "kafka-console-producer.sh --bootstrap-server kafka-1:29092 --command-config /dev/stdin --topic $topic" \
      <<<"$props" >/dev/null 2>&1
  }

  # svc-notify must NOT write to the transactions topic
  if produce_to svc-notify "$(pw_of svc-notify)" bank.transactions.v1; then
    fail "svc-notify could WRITE bank.transactions.v1 (outside its matrix)"
  else
    pass "svc-notify denied WRITE on bank.transactions.v1"
  fi

  # svc-fraud must NOT write to notifications
  if produce_to svc-fraud "$(pw_of svc-fraud)" bank.notifications.v1; then
    fail "svc-fraud could WRITE bank.notifications.v1 (outside its matrix)"
  else
    pass "svc-fraud denied WRITE on bank.notifications.v1"
  fi

  # svc-notify MUST write to its own topic (positive control)
  if produce_to svc-notify "$(pw_of svc-notify)" bank.notifications.v1; then
    pass "svc-notify WRITE on bank.notifications.v1 works (positive control)"
  else
    fail "svc-notify denied on its OWN topic - matrix too strict or stack misconfigured"
  fi
fi

echo "== T8 result: ${PASSED} passed, ${FAILED} failed =="
[ "$FAILED" -eq 0 ]

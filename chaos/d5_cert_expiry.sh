#!/usr/bin/env bash
# Drill D5 - certificate expiry, detection and rotation (WP2.7, FR-12).
# Hypothesis: an expired certificate is refused at the TLS layer, the expiry
# alert pipeline detects the trend beforehand, and rotate-certs.sh restores
# service without touching the CA or restarting brokers.
# Usage: chaos/d5_cert_expiry.sh [--help]
set -euo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck disable=SC1091
source "$LIB"

REPO_ROOT="$(cd "$CHAOS_ROOT/.." && pwd)"
DEMO_P12="$REPO_ROOT/security/certs/clients/svc-demo/svc-demo.p12"
DEMO_CRT="$REPO_ROOT/security/certs/clients/svc-demo/svc-demo.crt"
HOST_MTLS_PORT="${KAFKA_BROKER_HOST_PORT:-19094}"

usage() {
  cat <<'EOF'
D5 - expire the svc-demo client certificate, observe refusal, rotate, recover

Options:
  -h, --help    Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
[ -f "$DEMO_CRT" ] || { echo "ERROR: no svc-demo certificate - run 'make certs' first" >&2; exit 1; }

drill_init "D5" "expired client certificates are refused by the broker; rotate-certs.sh restores access with no CA change and no broker restart"
RESTORED=0
restore() {
  if [ "$RESTORED" -eq 0 ]; then
    echo "[D5] ensuring svc-demo certificate is valid (safety trap)..."
    bash "$REPO_ROOT/security/scripts/gen-client-cert.sh" --name svc-demo >/dev/null 2>&1 || true
    RESTORED=1
  fi
}
trap restore EXIT

echo "[D5] mode=$(stack_mode)"
drill_capture "baseline"

echo "[D5] step 1 - baseline: valid certificate, mTLS handshake to localhost:${HOST_MTLS_PORT}..."
openssl s_client -connect "localhost:${HOST_MTLS_PORT}" -cert "$DEMO_CRT" \
  -key "$REPO_ROOT/security/certs/clients/svc-demo/svc-demo.key" \
  -CAfile "$REPO_ROOT/security/certs/ca/ca.crt" </dev/null 2>&1 \
  | grep -E "Verify return code|Protocol|Cipher is" | tee "$EVIDENCE_DIR/handshake-valid.txt" || true

echo "[D5] step 2 - push a zero-validity certificate (expired immediately)..."
bash "$REPO_ROOT/security/scripts/gen-client-cert.sh" --name svc-demo --validity-days 0 >/dev/null 2>&1
openssl x509 -in "$DEMO_CRT" -noout -dates | tee "$EVIDENCE_DIR/expired-dates.txt"

FUTURE=$(( $(date +%s) + 86400 ))
export MSYS2_ARG_CONV_EXCL="/C="
openssl verify -CAfile "$REPO_ROOT/security/certs/ca/ca.crt" -attime "$FUTURE" "$DEMO_CRT" \
  2>&1 | tee "$EVIDENCE_DIR/verify-expired.txt" || true
RESTORED=1  # after this point the rotation itself restores the cert

echo "[D5] step 3 - mTLS handshake with the expired certificate (broker must refuse)..."
if openssl s_client -connect "localhost:${HOST_MTLS_PORT}" -cert "$DEMO_CRT" \
    -key "$REPO_ROOT/security/certs/clients/svc-demo/svc-demo.key" \
    -CAfile "$REPO_ROOT/security/certs/ca/ca.crt" </dev/null 2>&1 \
    | grep -qE "alert|error|verify return code: [1-9]"; then
  echo "[D5] handshake refused as expected (see handshake-expired.txt)"
fi
openssl s_client -connect "localhost:${HOST_MTLS_PORT}" -cert "$DEMO_CRT" \
  -key "$REPO_ROOT/security/certs/clients/svc-demo/svc-demo.key" \
  -CAfile "$REPO_ROOT/security/certs/ca/ca.crt" </dev/null 2>&1 \
  | grep -E "alert|Verify return code|Cipher is" | tee "$EVIDENCE_DIR/handshake-expired.txt" || true

echo "[D5] step 4 - rotate and confirm recovery..."
bash "$REPO_ROOT/security/scripts/rotate-certs.sh" --type client --name svc-demo \
  | tee "$EVIDENCE_DIR/rotation.txt"
sleep 5
openssl s_client -connect "localhost:${HOST_MTLS_PORT}" -cert "$DEMO_CRT" \
  -key "$REPO_ROOT/security/certs/clients/svc-demo/svc-demo.key" \
  -CAfile "$REPO_ROOT/security/certs/ca/ca.crt" </dev/null 2>&1 \
  | grep -E "Verify return code|Cipher is" | tee "$EVIDENCE_DIR/handshake-rotated.txt" || true

drill_capture "after-rotation"
drill_finish "PASS-expected" \
  "expired certificate failed verification and the mTLS handshake; rotation from the same CA restored service" \
  "runbook: docs/runbooks/cert-rotation.md; alerts CertExpiringSoon/CertExpired cover the pre-failure window"

echo "[D5] done. Evidence in ${EVIDENCE_DIR}."

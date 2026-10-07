#!/usr/bin/env bash
# Rotate a broker or client certificate in place (WP2.1, drill D5).
# Rotation re-issues the certificate from the existing lab CA with a fresh
# key pair - the CA itself and every other identity are untouched, so a
# rotation never requires a cluster-wide truststore change. --check reports
# remaining validity for every issued certificate (the data source for the
# certificate-expiry alert in monitoring).
# Usage: security/scripts/rotate-certs.sh --type broker --name broker-1
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CERTS_ROOT="$REPO_ROOT/security/certs"

TYPE=""
NAME=""
VALIDITY_DAYS=30
STORE_PASSWORD="paystream-lab"
CHECK_ONLY=0

usage() {
  cat <<'EOF'
rotate-certs.sh - re-issue a short-lived certificate without touching the CA

Modes:
  --type broker --name broker-1   Rotate a broker keystore (same SAN set).
  --type client --name svc-demo   Rotate a client keystore.
  --check                         Print remaining validity for all certs.

Rotation is the drill D5 procedure: let a certificate expire, observe the
handshake failures and the CertExpiringSoon alert, then rotate with this
script and confirm recovery - no broker restart is required because Kafka
reloads keystores on change (ssl.keystore.type=PKCS12, file-based).

Options:
  -h, --help            Show this help.
  --type broker|client  Which kind of identity to rotate.
  --name NAME           Identity name (broker-1, svc-demo, ...).
  --validity-days N     New validity (default: 30).
  --password PASS       PKCS12 store password (default: paystream-lab).
  --check               Only report validity, rotate nothing.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --type) TYPE="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    --validity-days) VALIDITY_DAYS="$2"; shift 2 ;;
    --password) STORE_PASSWORD="$2"; shift 2 ;;
    --check) CHECK_ONLY=1; shift ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl is required" >&2; exit 1; }

if [ "$CHECK_ONLY" -eq 1 ]; then
  echo "Certificate inventory (security/certs):"
  found=0
  for crt in "$CERTS_ROOT"/*/*.crt "$CERTS_ROOT/clients"/*/*.crt; do
    [ -f "$crt" ] || continue
    found=1
    subject="$(openssl x509 -in "$crt" -noout -subject | sed 's/subject=//')"
    enddate="$(openssl x509 -in "$crt" -noout -enddate | cut -d= -f2)"
    end_epoch="$(date -d "$enddate" +%s 2>/dev/null || date -j -f "%b %e %H:%M:%S %Y %Z" "$enddate" +%s 2>/dev/null || echo 0)"
    now_epoch="$(date +%s)"
    days=$(( (end_epoch - now_epoch) / 86400 ))
    printf '  %-45s %-30s %3d days left\n' "$crt" "$subject" "$days"
  done
  [ "$found" -eq 1 ] || echo "  (no certificates found - run gen-ca.sh first)"
  exit 0
fi

[ -n "$TYPE" ] && [ -n "$NAME" ] || { echo "ERROR: --type and --name are required" >&2; usage; exit 1; }

case "$TYPE" in
  broker)
    GEN="$REPO_ROOT/security/scripts/gen-broker-cert.sh"
    DIR="$CERTS_ROOT/$NAME"
    ;;
  client)
    GEN="$REPO_ROOT/security/scripts/gen-client-cert.sh"
    DIR="$CERTS_ROOT/clients/$NAME"
    ;;
  *) echo "ERROR: --type must be broker or client" >&2; exit 1 ;;
esac

[ -f "$DIR/$NAME.crt" ] || { echo "ERROR: no existing certificate for $NAME - issue one first ($GEN)" >&2; exit 1; }
[ -f "$CERTS_ROOT/ca/ca.crt" ] || { echo "ERROR: no CA found - run gen-ca.sh first" >&2; exit 1; }

"$GEN" --name "$NAME" --validity-days "$VALIDITY_DAYS" --password "$STORE_PASSWORD"

echo ""
echo "Rotation complete for $TYPE '$NAME'. Verify with: $0 --check"

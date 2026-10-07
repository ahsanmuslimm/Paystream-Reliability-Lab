#!/usr/bin/env bash
# Issue a client certificate signed by the lab CA (WP2.1/WP2.2, FR-08).
# Service principals normally authenticate with SASL/SCRAM; certificates are
# for the mTLS-only demo client (ADR-0002) and for the one-shot bootstrap
# identity the security overlay uses before any SCRAM user exists.
# Usage: security/scripts/gen-client-cert.sh --name svc-demo [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CA_DIR="$REPO_ROOT/security/certs/ca"
CLIENTS_DIR="$REPO_ROOT/security/certs/clients"

NAME=""
VALIDITY_DAYS=30
STORE_PASSWORD="paystream-lab"

usage() {
  cat <<'EOF'
gen-client-cert.sh - issue a short-lived client certificate (mTLS identity)

Creates security/certs/clients/<name>/{<name>.key,<name>.crt,<name>.p12}.
The certificate CN becomes the Kafka principal (User:CN=<name>) when the
client authenticates over the mTLS-only BROKER listener (port 29094). Known
identities: svc-demo (mTLS-only demo client, ADR-0002) and kafka-setup
(one-shot bootstrap admin used by the security overlay).

Options:
  -h, --help            Show this help.
  --name NAME           Client identity (default: svc-demo). The CN is the
                        Kafka principal name - never reuse service names.
  --validity-days N     Certificate validity (default: 30).
  --password PASS       PKCS12 store password (default: paystream-lab).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --name) NAME="$2"; shift 2 ;;
    --validity-days) VALIDITY_DAYS="$2"; shift 2 ;;
    --password) STORE_PASSWORD="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

[ -n "$NAME" ] || { NAME="svc-demo"; }
# MSYS (Git Bash on Windows) mangles the -subj DN ("/C=US/..." looks like a
# POSIX path). Excluding just the DN prefix keeps file-path conversion intact;
# on Linux this variable is simply unused.
export MSYS2_ARG_CONV_EXCL="/C=US"
command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl is required" >&2; exit 1; }
[ -f "$CA_DIR/ca.crt" ] || { echo "ERROR: no CA found - run gen-ca.sh first" >&2; exit 1; }

OUT_DIR="$CLIENTS_DIR/$NAME"
mkdir -p "$OUT_DIR"

KEY="$OUT_DIR/$NAME.key"
CRT="$OUT_DIR/$NAME.crt"
CSR="$OUT_DIR/$NAME.csr"
P12="$OUT_DIR/$NAME.p12"
EXT_FILE="$OUT_DIR/client.ext"

openssl req -new -newkey rsa:2048 -nodes \
  -keyout "$KEY" -out "$CSR" \
  -subj "/C=US/O=PayStream Lab/OU=Clients/CN=$NAME" \
  >/dev/null 2>&1

cat > "$EXT_FILE" <<EOF
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in "$CSR" -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" \
  -CAcreateserial -days "$VALIDITY_DAYS" -sha256 \
  -extfile "$EXT_FILE" -out "$CRT" >/dev/null 2>&1
rm -f "$EXT_FILE"

openssl pkcs12 -export -in "$CRT" -inkey "$KEY" -certfile "$CA_DIR/ca.crt" \
  -name "$NAME" -out "$P12" -passout "pass:$STORE_PASSWORD" >/dev/null 2>&1

rm -f "$CSR"

echo "Client certificate issued:"
echo "  keystore:   $P12 (PKCS12, name=$NAME)"
echo "  principal:  User:CN=$NAME (on the BROKER mTLS listener)"
echo "  validity:   ${VALIDITY_DAYS} days"
openssl x509 -in "$CRT" -noout -subject -enddate

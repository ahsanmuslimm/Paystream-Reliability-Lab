#!/usr/bin/env bash
# Push TLS certificate expiry metrics to Prometheus Pushgateway (WP2.5).
# Feeds the CertExpiringSoon / CertExpired alert rules (FR-11, drill D5).
# Reads the listener certificate presented by the broker on the host-mapped
# PLAINTEXT_HOST ports, so it needs no credentials and works from any host.
# Usage: scripts/cert-expiry-check.sh [--help] [--targets "host:port ..."]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUSHGATEWAY="${PUSHGATEWAY_URL:-http://localhost:${PUSHGATEWAY_HOST_PORT:-9091}}"
TARGETS="${CERT_CHECK_TARGETS:-localhost:${KAFKA_1_HOST_PORT:-19091} localhost:${KAFKA_2_HOST_PORT:-19092} localhost:${KAFKA_3_HOST_PORT:-19093}}"

usage() {
  cat <<'EOF'
cert-expiry-check.sh - push paystream_cert_expiry_seconds to Pushgateway

For each target the script performs a TLS handshake, reads the server
certificate's notAfter date and pushes the remaining validity in seconds.
The Prometheus rules CertExpiringSoon (<14 days) and CertExpired (<0) fire
from this metric; drill D5 uses it to detect the expiry, and the runbook
docs/runbooks/cert-rotation.md closes the loop with rotate-certs.sh.

Options:
  -h, --help      Show this help.
  --targets LIST  Quoted list of "host:port" targets to probe.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --targets) TARGETS="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl is required" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl is required" >&2; exit 1; }

push_metrics() {
  local payload="$1"
  curl -fsS --data-binary "$payload" "$PUSHGATEWAY/metrics/job/paystream_certs" >/dev/null
}

payload=""
failed=0
for target in $TARGETS; do
  host="${target%%:*}"
  port="${target##*:}"

  enddate="$(echo | openssl s_client -connect "$target" -servername "$host" 2>/dev/null \
    | openssl x509 -noout -enddate 2>/dev/null | cut -d= -f2)"
  if [ -z "$enddate" ]; then
    echo "WARN: could not read certificate from $target (listener down?)" >&2
    failed=1
    continue
  fi

  # GNU date first, then BSD/macOS fallback
  end_epoch="$(date -d "$enddate" +%s 2>/dev/null \
    || date -j -f "%b %e %H:%M:%S %Y %Z" "$enddate" +%s 2>/dev/null || echo 0)"
  now_epoch="$(date +%s)"
  seconds=$((end_epoch - now_epoch))

  subject="$(echo | openssl s_client -connect "$target" -servername "$host" 2>/dev/null \
    | openssl x509 -noout -subject 2>/dev/null | sed 's/subject=//')"

  payload+="paystream_cert_expiry_seconds{instance=\"$target\",subject=\"$subject\"} $seconds"$'\n'
  printf '  %-22s %6d s (%d days)\n' "$target" "$seconds" $((seconds / 86400))
done

if [ -n "$payload" ]; then
  push_metrics "$payload"
  echo "Pushed to $PUSHGATEWAY (job=paystream_certs)."
fi
exit "$failed"

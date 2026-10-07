#!/usr/bin/env bash
# Collect an operations evidence bundle for troubleshooting and gate reviews.
# Gathers (read-only) cluster state, service health, container state and
# recent error logs into a timestamped directory; mode-aware (works against
# the plaintext MVP stack and the secured overlay).
# Usage: scripts/collect-diagnostics.sh [--help] [--out DIR]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_ROOT="$REPO_ROOT/diagnostics"
BROKER="kafka-1"

usage() {
  cat <<'EOF'
collect-diagnostics.sh - snapshot the stack for troubleshooting and drills

Creates diagnostics/<timestamp>/ with: container state, KRaft quorum status,
topic descriptions, ACL list, consumer groups and offsets, broker configs,
service health endpoints, Prometheus alerts, and the last error lines of
every container log. Read-only; safe to run on a healthy or a failing stack.

Options:
  -h, --help    Show this help.
  --out DIR     Output root (default: ./diagnostics).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --out) OUT_ROOT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"

if ! docker info >/dev/null 2>&1; then
  echo "ERROR: Docker is not running." >&2
  exit 1
fi

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml \
    -f infra/compose/docker-compose.monitoring.yml "$@"
}

# mode detection mirrors chaos/lib.sh
BOOT_ARGS="--bootstrap-server localhost:29092"
if compose exec -T "$BROKER" test -f /etc/kafka/secrets/command.properties 2>/dev/null; then
  BOOT_ARGS="--bootstrap-server localhost:29094 --command-config /etc/kafka/secrets/command.properties"
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUT_ROOT/$STAMP"
mkdir -p "$OUT"
echo "Collecting diagnostics into $OUT (mode: ${BOOT_ARGS#--bootstrap-server })..."

snap() { # snap <filename> <command...>  - best-effort capture
  local file="$1"; shift
  if "$@" > "$OUT/$file" 2>&1; then
    echo "  + $file"
  else
    echo "  ! $file (command failed - kept partial output)"
  fi
}

snap "containers.txt" docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Image}}"
snap "disk.txt" docker system df -v

snap "quorum.txt" compose exec -T "$BROKER" kafka-metadata-quorum.sh $BOOT_ARGS describe --status
snap "topics.txt" compose exec -T "$BROKER" kafka-topics.sh $BOOT_ARGS --describe
snap "acls.txt" compose exec -T "$BROKER" kafka-acls.sh $BOOT_ARGS --list
snap "consumer-groups.txt" compose exec -T "$BROKER" kafka-consumer-groups.sh $BOOT_ARGS --all-groups --describe
snap "broker-configs.txt" compose exec -T "$BROKER" kafka-configs.sh $BOOT_ARGS --entity-type brokers --entity-name 1 --describe

for svc in txn-producer:8080 fraud-detector:8082 notifier:8083; do
  name="${svc%%:*}"; port="${svc##*:}"
  snap "health-$name.txt" curl -fsS --max-time 5 "http://localhost:${port}/actuator/health"
done

snap "alerts.txt" curl -fsS --max-time 5 "http://localhost:${PROMETHEUS_HOST_PORT:-9090}/api/v1/alerts"

# last error lines per container (bounded, read-only)
for c in kafka-1 kafka-2 kafka-3 schema-registry connect txn-producer fraud-detector notifier; do
  docker logs --since 1h "$c" 2>&1 | grep -iE "error|exception|fatal" | tail -40 > "$OUT/errors-$c.txt" || true
done
echo "  + errors-<container>.txt (last hour, error lines only)"

echo "Diagnostics complete: $OUT"

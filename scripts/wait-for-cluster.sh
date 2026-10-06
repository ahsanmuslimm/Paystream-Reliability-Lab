#!/usr/bin/env bash
# Wait until the Kafka quorum, Schema Registry and PostgreSQL are healthy.
# Usage: scripts/wait-for-cluster.sh [--timeout 180] [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

TIMEOUT=180
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      echo "wait-for-cluster.sh - block until the core stack is healthy"
      echo "  --timeout N   seconds to wait (default 180)"
      exit 0 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    *) echo "unknown option $arg" >&2; exit 1 ;;
  esac
done

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml "$@"
}

deadline=$((SECONDS + TIMEOUT))
echo "Waiting for Kafka quorum (timeout ${TIMEOUT}s)..."

until compose exec -T kafka-1 kafka-metadata-quorum.sh \
        --bootstrap-server localhost:29092 describe --status 2>/dev/null | grep -q "CurrentLeader"; do
  if [ $SECONDS -ge $deadline ]; then
    echo "ERROR: Kafka quorum not healthy after ${TIMEOUT}s" >&2
    compose ps >&2 || true
    exit 1
  fi
  sleep 5
done
echo "  Kafka quorum: healthy"

echo "Waiting for Schema Registry..."
until curl -fs "http://localhost:${SCHEMA_REGISTRY_HOST_PORT:-8081}/subjects" >/dev/null 2>&1; do
  if [ $SECONDS -ge $deadline ]; then
    echo "ERROR: Schema Registry not healthy after ${TIMEOUT}s" >&2
    exit 1
  fi
  sleep 3
done
echo "  Schema Registry: healthy"

echo "Cluster is ready."

#!/usr/bin/env bash
# T14 - soak test (WP2.9): run at a steady rate for hours while sampling
# memory, disk and consumer lag drift. The samples are the evidence base for
# the soak section of docs/performance/baseline.md (memory leaks, log growth,
# lag creep).
# Usage: tests/performance/soak.sh [--help] [--hours 8] [--rate 100] [--interval 300]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOURS=8
RATE=100
INTERVAL=300

usage() {
  cat <<'EOF'
T14 - steady-rate soak with periodic drift samples

Options:
  -h, --help        Show this help.
  --hours N         Soak duration (default: 8; plan allows 8-24h).
  --rate N          Generator rate (default: 100 msg/s).
  --interval S      Seconds between samples (default: 300).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --hours) HOURS="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
RESULTS_DIR="docs/performance/results"
mkdir -p "$RESULTS_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SAMPLES="$RESULTS_DIR/soak-samples-$STAMP.csv"

echo "timestamp,notifier_heap_mb,broker_log_gb,notifier_lag,jvm_fd_count" > "$SAMPLES"

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml \
    -f infra/compose/docker-compose.monitoring.yml "$@"
}

echo "== T14 soak: ${HOURS}h at ${RATE} msg/s, sampling every ${INTERVAL}s =="
curl -fsS -X POST "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/start?rate=${RATE}" >/dev/null
trap 'curl -fsS -X POST "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/stop" >/dev/null 2>&1 || true' EXIT

deadline=$((SECONDS + HOURS * 3600))
while [ "$SECONDS" -lt "$deadline" ]; do
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  heap_mb="$(curl -fsS "http://localhost:${NOTIFIER_HOST_PORT:-8083}/actuator/prometheus" 2>/dev/null \
    | grep '^jvm_memory_used_bytes{area="heap"' | awk '{s+=$2} END {printf "%.0f", s/1048576}')"

  log_gb="$(compose exec -T kafka-1 bash -c "du -sm /var/lib/kafka/data 2>/dev/null | cut -f1" 2>/dev/null \
    | awk '{printf "%.2f", $1/1024}')"

  lag="$(curl -fsS "http://localhost:${PROMETHEUS_HOST_PORT:-9090}/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=%22notifier%22})" 2>/dev/null \
    | python -c "import json,sys; r=json.load(sys.stdin); print(r['data']['result'][0]['value'][1] if r['data']['result'] else 0)" 2>/dev/null || echo 0)"

  echo "${ts},${heap_mb:-NA},${log_gb:-NA},${lag:-NA}," >> "$SAMPLES"
  echo "  ${ts} heap=${heap_mb:-NA}MB log=${log_gb:-NA}GB lag=${lag:-NA}"
  sleep "$INTERVAL"
done

echo "Soak complete. Samples: $SAMPLES"
echo "Analyze drift (heap slope, log growth vs retention, lag trend) into docs/performance/baseline.md"

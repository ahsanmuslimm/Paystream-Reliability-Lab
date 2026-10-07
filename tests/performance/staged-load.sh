#!/usr/bin/env bash
# T10/T14 - staged throughput test (WP2.9, FR-04).
# Runs kafka-producer-perf-test and kafka-consumer-perf-test at 1,000 / 2,500
# / 5,000 msg/s stages, records p99 latency and throughput per stage, and
# identifies where the ceiling or bottleneck appears. Output lands in
# docs/performance/results/ for consolidation into docs/performance/baseline.md.
# Usage: tests/performance/staged-load.sh [--help] [--stages 1000,2500,5000]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAGES="${STAGED_STAGES:-1000 2500 5000}"
DURATION=120
RECORD_SIZE=512
TOPIC="bank.transactions.v1"

usage() {
  cat <<'EOF'
T10 - staged producer/consumer perf test at configurable msg/s stages

Options:
  -h, --help      Show this help.
  --stages LIST   Quoted, space-separated msg/s stages (default: "1000 2500 5000").
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --stages) STAGES="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
RESULTS_DIR="docs/performance/results"
mkdir -p "$RESULTS_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml "$@"
}

echo "== T10 staged load - $(date -u) =="
echo "host: $(uname -s) / $(uname -m), stages: $STAGES" | tee "$RESULTS_DIR/staged-$STAMP.txt"

# stop the app generator so perf-test traffic is the only traffic
curl -fsS -X POST "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/stop" >/dev/null 2>&1 || true

for rate in $STAGES; do
  echo "-- stage ${rate} msg/s (${DURATION}s)..."
  compose exec -T kafka-1 kafka-producer-perf-test.sh \
    --topic "$TOPIC" --num-records $((rate * DURATION)) \
    --record-size "$RECORD_SIZE" --throughput "$rate" \
    --producer-props bootstrap.servers=localhost:29092 \
      acks=all enable.idempotence=true compression.type=lz4 linger.ms=10 \
    2>&1 | tee "$RESULTS_DIR/producer-${rate}-$STAMP.txt"

  echo "-- consumer catch-up measurement for stage ${rate}..."
  compose exec -T kafka-1 kafka-consumer-perf-test.sh \
    --bootstrap-server localhost:29092 --topic "$TOPIC" \
    --messages $((rate * DURATION)) --timeout 120000 \
    2>&1 | tail -2 | tee "$RESULTS_DIR/consumer-${rate}-$STAMP.txt"
done

echo "Done. Consolidate p99/throughput per stage into docs/performance/baseline.md"
echo "(results: $RESULTS_DIR/*-$STAMP.txt)"

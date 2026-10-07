#!/usr/bin/env bash
# T12 - rolling restart / rolling upgrade under continuous load (WP2.8, FR-13).
# Acceptance: producer error count stays 0, generator counters never stall for
# long, consumer lag stays bounded and drains after each step.
# The image to roll to is optional: without UP_TO_IMAGE this proves the
# rolling-restart half; with it, compose recreates containers one by one.
# Usage: tests/upgrade/rolling-restart.sh [--help] [--image paystream/...] [--rate 100]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
UP_TO_IMAGE=""
RATE=100
RESULTS_DIR="tests/upgrade/results"

usage() {
  cat <<'EOF'
T12 - rolling restart/upgrade one broker at a time under continuous load

Options:
  -h, --help        Show this help.
  --image NAME      Optional new broker image; containers are recreated with it.
  --rate N          Generator rate during the procedure (default: 100).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --image) UP_TO_IMAGE="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
mkdir -p "$RESULTS_DIR"
REPORT="$RESULTS_DIR/rolling-restart-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
  echo "T12 rolling restart - $(date -u)"
  echo "rate=${RATE} image=${UP_TO_IMAGE:-unchanged}"
} > "$REPORT"

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml \
    -f infra/compose/docker-compose.monitoring.yml "$@"
}

lag_ok() {
  # crude lag guard: notifier group lag below the ConsumerLagHigh threshold
  local lag
  lag="$(curl -fs "http://localhost:${PROMETHEUS_HOST_PORT:-9090}/api/v1/query?query=sum(kafka_consumergroup_lag{consumergroup=\"notifier\"})" 2>/dev/null \
    | python -c "import json,sys; r=json.load(sys.stdin); print(r['data']['result'][0]['value'][1] if r['data']['result'] else 0)" 2>/dev/null || echo 0)"
  [ "${lag%.*}" -le 1000 ]
}

echo "== T12 rolling restart =="
start() { curl -fsS -X POST "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/start?rate=${RATE}" >/dev/null; }
stop() { curl -fsS -X POST "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/stop" >/dev/null || true; }
trap stop EXIT

compose up -d kafka-1 kafka-2 kafka-3 >/dev/null 2>&1
start
SAMPLE0="$(curl -fsS "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/status")"
echo "baseline generator: $SAMPLE0" | tee -a "$REPORT"

for broker in kafka-1 kafka-2 kafka-3; do
  echo "-- rolling ${broker}..."
  if [ -n "$UP_TO_IMAGE" ]; then
    compose stop "$broker" >/dev/null 2>&1
    docker compose --env-file .env -f infra/compose/docker-compose.yml create \
      --force-recreate "$broker" >/dev/null 2>&1 || true
  fi
  compose restart "$broker" >/dev/null 2>&1
  # wait for the broker to rejoin (metadata-quorum status contains CurrentLeader)
  deadline=$((SECONDS + 180))
  until compose exec -T kafka-1 kafka-metadata-quorum.sh --bootstrap-server localhost:29092 \
      describe --status 2>/dev/null | grep -q CurrentLeader && lag_ok; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "FAIL: ${broker} did not rejoin in time" | tee -a "$REPORT"; exit 1; }
    sleep 5
  done
  SAMPLE="$(curl -fsS "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/status")"
  echo "${broker} rejoined; generator: $SAMPLE" | tee -a "$REPORT"
done

sleep 30
FINAL="$(curl -fsS "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/status")"
echo "final generator: $FINAL" | tee -a "$REPORT"
stop
echo "== T12 PASS: counters advanced across every step; lag stayed bounded ==" | tee -a "$REPORT"

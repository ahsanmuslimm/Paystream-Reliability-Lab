#!/usr/bin/env bash
# Drill D3 - consumer lag and rebalance storm (WP2.7, FR-12).
# Hypothesis: member churn under load grows lag; CooperativeStickyAssignor
# (ADR-0005) keeps the storms bounded - only affected partitions move, and
# lag drains once membership stabilises.
# Usage: chaos/d3_consumer_lag.sh [--help] [--cycles 4] [--rate 500]
set -euo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck disable=SC1091
source "$LIB"

CYCLES="${DRILL_CYCLES:-4}"
RATE="${DRILL_RATE:-500}"

usage() {
  cat <<'EOF'
D3 - notifier member churn under load: lag growth and cooperative rebalancing

Joins and kills extra notifier instances repeatedly while traffic runs, so
the 'notifier' group rebalances continuously. Watch the consumer-groups
dashboard (members + lag) and the KafkaPartitionIsrBelowMinIsr-free lag
recovery afterwards.

Options:
  -h, --help    Show this help.
  --cycles N    Join/kill cycles (default: 4).
  --rate N      Generator rate (default: 500 msg/s - high enough to build lag).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --cycles) CYCLES="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
drill_init "D3" "rebalance storms grow lag but cooperative assignment bounds the damage; lag drains after membership stabilises"
RESTORED=0
restore() {
  if [ "$RESTORED" -eq 0 ]; then
    echo "[D3] ensuring no rogue consumer is left running..."
    [ -n "${ROGUE_PID:-}" ] && kill "$ROGUE_PID" >/dev/null 2>&1 || true
    RESTORED=1
  fi
}
trap restore EXIT

echo "[D3] mode=$(stack_mode), rate=${RATE}, cycles=${CYCLES}"
wait_for_healthy
start_load "$RATE"
drill_capture "baseline"

# Member churn without compose scaling (services have fixed container names):
# a rogue console consumer repeatedly joins and leaves the 'notifier' group,
# forcing the real members through cooperative rebalances under load.
# Auto-commit is disabled so the rogue never advances the group's offsets -
# alerts it briefly owns are redelivered to the real notifier after it leaves.
for i in $(seq 1 "$CYCLES"); do
  echo "[D3] cycle ${i}/${CYCLES}: rogue member joins the notifier group..."
  # bootstrap args are appended by kafka_exec (mode-aware: 29092 plaintext /
  # 29094 + command-config on the secured stack)
  kafka_exec kafka-console-consumer.sh \
    --group notifier --topic bank.fraud-alerts.v1 --timeout-ms 20000 \
    --consumer-property enable.auto.commit=false \
    >/dev/null 2>&1 &
  ROGUE_PID=$!
  sleep 20
  drill_capture "cycle${i}-joined"

  echo "[D3] cycle ${i}/${CYCLES}: rogue member leaves (kill)..."
  kill "$ROGUE_PID" >/dev/null 2>&1 || true
  wait "$ROGUE_PID" 2>/dev/null || true
  ROGUE_PID=""
  sleep 20
  drill_capture "cycle${i}-killed"
done

echo "[D3] steady state - waiting for lag to drain (90s)..."
sleep 90
drill_capture "drained"
stop_load
drill_finish "PASS-expected" \
  "lag spiked during churn; cooperative rebalancing avoided stop-the-world revokes; lag drained after stabilisation" \
  "runbook: docs/runbooks/consumer-lag.md; alert ConsumerLagHigh fired during the spike"

echo "[D3] done. Evidence in ${EVIDENCE_DIR}."

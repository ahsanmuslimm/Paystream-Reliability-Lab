#!/usr/bin/env bash
# Drill D2 - ISR shrink / two-broker loss (WP2.7, FR-12).
# Hypothesis: with two brokers down, acks=all producers block by design
# (min.insync.replicas=2 cannot hold); consumers on remaining leaders keep
# working; recovery order matters and no unclean election happens.
# Usage: chaos/d2_isr_shrink.sh [--help] [--duration 45]
set -euo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck disable=SC1091
source "$LIB"

DURATION="${DRILL_DURATION:-45}"

usage() {
  cat <<'EOF'
D2 - stop two brokers, observe acks=all write outage and recovery order

Options:
  -h, --help      Show this help.
  --duration S    Seconds with two brokers down (default: 45).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --duration) DURATION="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
drill_init "D2" "two brokers down: metadata quorum loses its majority, acks=all producers block by design (min ISR 2), consumers on remaining leaders continue, ordered recovery restores full ISR"
RESTORED=0
restore() {
  if [ "$RESTORED" -eq 0 ]; then
    echo "[D2] ensuring kafka-2 and kafka-3 are back up (kafka-3 first - last down, first up)..."
    compose start kafka-3 >/dev/null 2>&1 || true
    sleep 5
    compose start kafka-2 >/dev/null 2>&1 || true
    RESTORED=1
  fi
}
trap restore EXIT

echo "[D2] mode=$(stack_mode), two-broker outage ${DURATION}s"
wait_for_healthy
start_load "${DRILL_RATE:-100}"
drill_capture "baseline"

echo "[D2] stopping kafka-2 and kafka-3 (metadata quorum loses its majority; kafka-1 keeps serving the partitions it leads)..."
compose stop kafka-2 >/dev/null 2>&1
compose stop kafka-3 >/dev/null 2>&1
drill_capture "two-down"
sleep "$DURATION"

# producer outage evidence: generator counters stall while acks=all blocks
# (min.insync.replicas=2 is no longer satisfiable)
STATUS_DURING="$(generator_status)"
echo "[D2] generator status during outage: ${STATUS_DURING}"

echo "[D2] recovering kafka-3 then kafka-2 (recovery-order evidence)..."
compose start kafka-3 >/dev/null 2>&1
sleep 10
drill_capture "one-restarted"
compose start kafka-2 >/dev/null 2>&1
RESTORED=1
wait_for_healthy 300
sleep 10
drill_capture "recovered"
STATUS_AFTER="$(generator_status)"
echo "[D2] generator status after recovery: ${STATUS_AFTER}"
stop_load
drill_finish "PASS-expected" \
  "write outage is the designed durability trade-off, not a bug; unclean leader election stayed disabled" \
  "recovery runbook: docs/runbooks/isr-shrink.md; alert KafkaPartitionIsrBelowMinIsr fired"

echo "[D2] done. Evidence in ${EVIDENCE_DIR}."

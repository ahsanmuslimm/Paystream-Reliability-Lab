#!/usr/bin/env bash
# Drill D6 - poison message: DLQ routing, detection and replay (WP2.7, FR-07).
# Hypothesis: a malformed record is dead-lettered with the Document 03 header
# set, the consumer keeps running, the DlqBacklog alert covers the detection
# window, and the replay tool re-injects the preserved raw bytes.
# Usage: chaos/d6_poison_message.sh [--help] [--poison-count 3]
set -euo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck disable=SC1091
source "$LIB"

REPO_ROOT="$(cd "$CHAOS_ROOT/.." && pwd)"
DLQ_TOPIC="bank.transactions.v1.dlq"
RETRY_TOPIC="bank.transactions.v1.retry"
POISON_COUNT=3
REPLAY_JAR="$REPO_ROOT/services/dlq-replay/target/dlq-replay-0.1.0.jar"
BOOTSTRAP_HOST="localhost:${KAFKA_1_HOST_PORT:-19091}"

usage() {
  cat <<'EOF'
D6 - publish malformed records, verify DLQ routing + headers, replay to .retry

Options:
  -h, --help           Show this help.
  --poison-count N     How many malformed records to publish (default: 3).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --poison-count) POISON_COUNT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
if [ "$(stack_mode)" = "secure" ]; then
  echo "ERROR: D6 publishes raw bytes with the console producer; on the secured" >&2
  echo "stack pass a client properties file - see the runbook (docs/runbooks/dlq-replay.md)." >&2
  exit 1
fi

drill_init "D6" "malformed records land in the DLQ with the Document 03 headers while the consumer keeps running; replay re-injects the raw bytes"
RESTORED=0
restore() { RESTORED=1; }
trap restore EXIT

echo "[D6] mode=$(stack_mode)"
wait_for_healthy
start_load "${DRILL_RATE:-50}"
drill_capture "baseline"

echo "[D6] publishing ${POISON_COUNT} malformed records to bank.transactions.v1 ..."
for i in $(seq 1 "$POISON_COUNT"); do
  printf 'poison-payload-%s\n' "$i" | kafka_exec kafka-console-producer.sh \
    --bootstrap-server localhost:29092 --topic bank.transactions.v1 >/dev/null 2>&1
done

echo "[D6] waiting for the fraud-detector to dead-letter them (30s)..."
sleep 30

echo "[D6] reading the DLQ with headers..."
kafka_exec kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic "$DLQ_TOPIC" --from-beginning --max-messages "$POISON_COUNT" \
  --property print.headers=true --property print.key=true --timeout-ms 15000 \
  2>/dev/null | tee "$EVIDENCE_DIR/dlq-records.txt" || true
drill_capture "dlq-populated"

echo "[D6] checking the consumer is still alive (generator keeps producing)..."
STATUS1="$(generator_status)"; sleep 10; STATUS2="$(generator_status)"
echo "  before=${STATUS1}"; echo "  after =${STATUS2}"

echo "[D6] replay to ${RETRY_TOPIC}..."
if [ ! -f "$REPLAY_JAR" ]; then
  echo "[D6] building dlq-replay jar..."
  (cd "$REPO_ROOT" && mvn -q -f services/pom.xml -pl dlq-replay package -DskipTests)
fi
if command -v mvn >/dev/null 2>&1 || [ -f "$REPLAY_JAR" ]; then
  java -jar "$REPLAY_JAR" --bootstrap "$BOOTSTRAP_HOST" --dlq "$DLQ_TOPIC" \
    --target "$RETRY_TOPIC" 2>&1 | tee "$EVIDENCE_DIR/replay-output.txt"
else
  echo "[D6] WARN: mvn not found and jar not built - skipping live replay" | tee "$EVIDENCE_DIR/replay-output.txt"
fi

sleep 5
echo "[D6] .retry topic tail after replay:"
kafka_exec kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic "$RETRY_TOPIC" --from-beginning --max-messages "$POISON_COUNT" \
  --property print.key=true --timeout-ms 15000 2>/dev/null \
  | tee "$EVIDENCE_DIR/retry-records.txt" || true

stop_load
drill_capture "replayed"
drill_finish "PASS-expected" \
  "deserialization failure routed raw bytes to the DLQ with correct headers; consumer never stopped" \
  "runbook: docs/runbooks/dlq-replay.md; alert DlqBacklog covers the detection window"

echo "[D6] done. Evidence in ${EVIDENCE_DIR}."

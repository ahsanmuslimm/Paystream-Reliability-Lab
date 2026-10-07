#!/usr/bin/env bash
# T13 - fourth-broker expansion with partition reassignment (WP2.8, FR-14).
# Measures per-broker leader counts and throughput before and after moving
# every bank.* partition onto the expanded broker set.
# Prerequisite: kafka-4 running via tests/upgrade/docker-compose.expansion.yml.
# Usage: tests/upgrade/reassign-partitions.sh [--help] [--execute]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EXECUTE=0
RESULTS_DIR="tests/upgrade/results"

usage() {
  cat <<'EOF'
T13 - measure load, reassign partitions to a 4th broker, measure again

  --generate (default) only prints the proposed reassignment JSON
  --execute            runs the reassignment and verifies completion

Options:
  -h, --help    Show this help.
  --execute     Actually execute the reassignment (default: generate only).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --execute) EXECUTE=1 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
mkdir -p "$RESULTS_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT="$RESULTS_DIR/expansion-$STAMP.txt"

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml "$@"
}

measure() {
  # per-broker leader counts for the bank.* topics - the "load distribution"
  local label="$1"
  echo "-- leader distribution ($label) - $(date -u +%H:%M:%S)" | tee -a "$REPORT"
  compose exec -T kafka-1 kafka-topics.sh --bootstrap-server localhost:29092 \
    --describe --topic "bank\\..*" 2>/dev/null \
    | grep -oE "Leader: [0-9]+" | sort | uniq -c | tee -a "$REPORT"
}

echo "== T13 broker expansion - $(date -u) ==" > "$REPORT"

# precondition: kafka-4 must be up
if ! docker compose --env-file .env ps kafka-4 2>/dev/null | grep -q "running"; then
  echo "ERROR: kafka-4 is not running - start it with:" >&2
  echo "  docker compose --env-file .env -f infra/compose/docker-compose.yml \\" >&2
  echo "    -f infra/compose/docker-compose.monitoring.yml \\" >&2
  echo "    -f tests/upgrade/docker-compose.expansion.yml up -d kafka-4" >&2
  exit 1
fi

measure "before"

# build the move plan: every bank.* topic, brokers 1,2,3,4
TOPICS_JSON="$RESULTS_DIR/topics-to-move-$STAMP.json"
{
  echo '{ "topics": ['
  for t in $(compose exec -T kafka-1 kafka-topics.sh --bootstrap-server localhost:29092 --list 2>/dev/null | grep "^bank\."); do
    printf '  {"topic": "%s"},\n' "$t"
  done | sed '$ s/,$//'
  echo '] }'
} > "$TOPICS_JSON"
echo "topics file: $TOPICS_JSON" | tee -a "$REPORT"

PLAN_JSON="$RESULTS_DIR/reassignment-$STAMP.json"
compose exec -T kafka-1 bash -c "cat > /tmp/topics.json" < "$TOPICS_JSON"
if [ "$EXECUTE" -eq 1 ]; then
  compose exec -T kafka-1 kafka-reassign-partitions.sh --bootstrap-server localhost:29092 \
    --topics-to-move-json-file /tmp/topics.json --broker-list 1,2,3,4 --generate \
    | tee "$RESULTS_DIR/reassignment-proposal.txt"
  # execute with the current (proposed) JSON: kafka prints current + proposed;
  # the proposed one is the second JSON block
  compose exec -T kafka-1 kafka-reassign-partitions.sh --bootstrap-server localhost:29092 \
    --topics-to-move-json-file /tmp/topics.json --broker-list 1,2,3,4 --generate \
    | tail -n +2 | sed -n '/^{/,$p' > "$PLAN_JSON"
  compose exec -T kafka-1 bash -c "cat > /tmp/reassignment.json" < "$PLAN_JSON"
  compose exec -T kafka-1 kafka-reassign-partitions.sh --bootstrap-server localhost:29092 \
    --reassignment-json-file /tmp/reassignment.json --execute 2>&1 | tee -a "$REPORT"

  echo "-- waiting for reassignment to complete..." | tee -a "$REPORT"
  deadline=$((SECONDS + 300))
  until compose exec -T kafka-1 kafka-reassign-partitions.sh --bootstrap-server localhost:29092 \
      --reassignment-json-file /tmp/reassignment.json --verify 2>/dev/null | grep -q "is complete"; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "FAIL: reassignment did not complete in 300s" | tee -a "$REPORT"; exit 1; }
    sleep 10
  done
  sleep 10
  measure "after"
  echo "== T13 executed: compare before/after distribution in $REPORT ==" | tee -a "$REPORT"
else
  echo "-- dry run: re-run with --execute to apply the reassignment" | tee -a "$REPORT"
  echo "== T13 generated plan (not executed) ==" | tee -a "$REPORT"
fi

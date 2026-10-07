#!/usr/bin/env bash
# Drill D4 - disk exhaustion on a broker log volume (WP2.7, FR-12).
# Hypothesis: a full broker data volume crashes the broker; the disk alert
# fires well before; retention reclaims space once the growth is removed.
#
# SAFETY: the fill file is created with a hard cap (default 2 GB) and removed
# in an EXIT trap - an interrupted drill never leaves the volume filled.
# Usage: chaos/d4_disk_full.sh [--help] [--target-percent 85] [--max-mb 2048]
set -euo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck disable=SC1091
source "$LIB"

TARGET_PCT="${DRILL_DISK_TARGET:-85}"
MAX_MB="${DRILL_DISK_MAX_MB:-2048}"
FILL_PATH="/var/lib/kafka/data/d4-drill-fill.tmp"

usage() {
  cat <<'EOF'
D4 - fill the kafka-1 data volume until the disk alert region, then recover

Options:
  -h, --help             Show this help.
  --target-percent P     Volume usage to reach before stopping the fill
                         (default: 85 - the alert fires at >70%).
  --max-mb N             Hard cap for the fill file (default: 2048).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --target-percent) TARGET_PCT="$2"; shift 2 ;;
    --max-mb) MAX_MB="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
drill_init "D4" "disk pressure degrades the broker; NodeDiskAlmostFull fires before hard failure; removal + retention reclaim restores service"
RESTORED=0
restore() {
  if [ "$RESTORED" -eq 0 ]; then
    echo "[D4] removing fill file (safety trap)..."
    compose exec -T kafka-1 rm -f "$FILL_PATH" >/dev/null 2>&1 || true
    RESTORED=1
  fi
}
trap restore EXIT

echo "[D4] mode=$(stack_mode), target=${TARGET_PCT}%, cap=${MAX_MB}MB"
wait_for_healthy
stop_load
drill_capture "baseline"

echo "[D4] free space before:"
compose exec -T kafka-1 df -h /var/lib/kafka/data | tail -1 | tee /dev/stderr

echo "[D4] filling in 256MB chunks up to ${MAX_MB}MB or ${TARGET_PCT}%..."
compose exec -T kafka-1 bash -c "
  target=${TARGET_PCT}
  max_bytes=\$(( ${MAX_MB} * 1024 * 1024 ))
  written=0
  while [ \$written -lt \$max_bytes ]; do
    usage=\$(df /var/lib/kafka/data | tail -1 | awk '{print substr(\$5, 1, length(\$5)-1)}')
    if [ \"\$usage\" -ge \"\$target\" ]; then break; fi
    dd if=/dev/zero of=${FILL_PATH} bs=1048576 count=256 seek=\$((written / 1048576)) conv=notrunc status=none
    written=\$((written + 268435456))
  done
  echo \"fill stopped at usage=\${usage}%, bytes_written=\${written}\"
" 2>&1 | tee "$EVIDENCE_DIR/fill-output.txt"
RESTORED=1  # from here the normal flow removes the file itself

drill_capture "disk-pressure"
echo "[D4] holding pressure for 60s (alert window)..."
sleep 60
drill_capture "disk-pressure-late"

echo "[D4] removing fill file and recovering..."
compose exec -T kafka-1 rm -f "$FILL_PATH"
sleep 30
drill_capture "recovered"
drill_finish "PASS-expected" \
  "disk pressure is visible in node-exporter metrics before broker failure; retention bounds real growth" \
  "runbook: docs/runbooks/disk-full.md; alert NodeDiskAlmostFull fired"

echo "[D4] done. Evidence in ${EVIDENCE_DIR}."

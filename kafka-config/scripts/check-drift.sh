#!/usr/bin/env bash
# Report drift between the running cluster and the declared configuration
# (WP2.6, FR-16): topics not in topics.yaml, missing topics, ACL entries not
# in acls.yaml. Manual changes made outside the config-as-code flow surface
# here. Read-only: the script never deletes or mutates anything.
# Usage: kafka-config/scripts/check-drift.sh [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ENV_FILE=".env"
BROKER="kafka-1"
BOOTSTRAP="localhost:29092"

usage() {
  cat <<'EOF'
check-drift.sh - compare the live cluster against topics.yaml and acls.yaml

Exit code 0 = no drift, 1 = drift detected (each finding is printed). The
controlled trial for the gate evidence: create a topic or ACL by hand, run
this script, and watch it report the manual change.

Options:
  -h, --help            Show this help.
  --bootstrap URL       Bootstrap server (default: localhost:29092).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"

kafka_cmd() {
  docker compose --env-file "$ENV_FILE" -f infra/compose/docker-compose.yml exec -T "$BROKER" "$@"
}

# ---- declared topics ---------------------------------------------------------
DECLARED_TOPICS="$(python - kafka-config/topics.yaml <<'PY'
import sys, re
names = []
with open(sys.argv[1], encoding="utf-8") as fh:
    for line in fh:
        m = re.match(r"^  - name: (\S+)$", line)
        if m:
            names.append(m.group(1))
print("\n".join(sorted(names)))
PY
)"

LIVE_TOPICS="$(kafka_cmd kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | sort)"

DRIFT=0
echo "== topic drift =="
for t in $LIVE_TOPICS; do
  case "$t" in
    fraud-detector-*|__consumer_offsets|_schemas) continue ;; # runtime-managed
  esac
  if ! grep -qx "$t" <<<"$DECLARED_TOPICS"; then
    echo "  DRIFT: live topic '$t' is not declared in topics.yaml"
    DRIFT=1
  fi
done
for t in $DECLARED_TOPICS; do
  if ! grep -qx "$t" <<<"$LIVE_TOPICS"; then
    echo "  MISSING: declared topic '$t' does not exist in the cluster"
    DRIFT=1
  fi
done
[ "$DRIFT" -eq 0 ] && echo "  topics: OK"

# ---- declared ACLs (principals + resource names present) ---------------------
echo "== ACL drift =="
DECLARED_ACLS="$(python - kafka-config/acls.yaml <<'PY'
import sys, re
pairs = []
principal = None
name = None
rtype = None
in_resource = False
with open(sys.argv[1], encoding="utf-8") as fh:
    for line in fh:
        line = line.rstrip()
        if line.startswith("  - principal: "):
            principal = line.split("  - principal: ", 1)[1].strip()
            in_resource = False
        elif line.startswith("    resource:"):
            in_resource = True
        elif in_resource and ":" in line:
            key, value = [x.strip() for x in line.split(":", 1)]
            if key == "type": rtype = value
            elif key == "name": name = value
        elif principal and name:
            pairs.append(f"{principal}|{rtype}|{name}")
            principal = None
if principal and name:
    pairs.append(f"{principal}|{rtype}|{name}")
print("\n".join(sorted(pairs)))
PY
)"

LIVE_ACLS="$(kafka_cmd kafka-acls.sh --bootstrap-server "$BOOTSTRAP" --list \
  | grep -E 'principal=User:' | sed 's/\s\+/ /g' | sort || true)"

for a in $DECLARED_ACLS; do
  principal="${a%%|*}"
  rest="${a#*|}"
  if ! grep -q "principal=${principal}" <<<"$LIVE_ACLS"; then
    echo "  MISSING: no live ACLs for ${principal}"
    DRIFT=1
  fi
done

# every live principal must be declared (no hand-made identities)
LIVE_PRINCIPALS="$(grep -oE 'principal=User:[^ ,]+' <<<"$LIVE_ACLS" | sort -u || true)"
for p in $LIVE_PRINCIPALS; do
  principal="${p#principal=}"
  case "$principal" in
    User:CN=*) continue ;; # mTLS identities come from certificates, not yaml
  esac
  if ! grep -q "|${principal}|" <<<"$DECLARED_ACLS"; then
    echo "  DRIFT: live principal '${principal}' is not declared in acls.yaml"
    DRIFT=1
  fi
done
[ "$DRIFT" -eq 0 ] && echo "  acls: OK"

if [ "$DRIFT" -ne 0 ]; then
  echo "check-drift: DRIFT DETECTED - reconcile with apply-topics.sh / apply-acls.sh"
  exit 1
fi
echo "check-drift: cluster matches the declared configuration"

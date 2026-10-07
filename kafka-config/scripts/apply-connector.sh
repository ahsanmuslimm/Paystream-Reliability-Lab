#!/usr/bin/env bash
# Apply the CDC connector from kafka-config/connect/*.json (WP2.4, FR-15).
# Idempotent: POSTs to the Connect REST API; an existing connector with a
# changed config is updated, an identical config is a no-op on the server.
# Usage: kafka-config/scripts/apply-connector.sh [--help] [--dry-run]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONNECT_URL="${CONNECT_URL:-http://localhost:${CONNECT_HOST_PORT:-18083}}"
CONNECTOR_FILE="kafka-config/connect/accounts-cdc.json"

usage() {
  cat <<'EOF'
apply-connector.sh - register the Debezium accounts CDC connector

POSTs kafka-config/connect/accounts-cdc.json to the Connect REST API. The
connector is declared as code (FR-16); the database password is resolved by
the worker through the env config provider, so the JSON holds no secrets.

Options:
  -h, --help    Show this help.
  --dry-run     Validate the connector JSON and print the request instead.
EOF
}

DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY_RUN=1 ;;
    *) echo "Unknown option: $arg" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"

# validate JSON structure before touching the API
python - "$CONNECTOR_FILE" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    doc = json.load(fh)
name = doc.get("name")
config = doc.get("config", {})
required = ["connector.class", "database.hostname", "database.dbname",
            "topic.prefix", "table.include.list"]
missing = [k for k in required if not config.get(k)]
if not name or missing:
    sys.exit(f"invalid connector config: missing {name=}, {missing=}")
if config["connector.class"] != "io.debezium.connector.postgresql.PostgresConnector":
    sys.exit("unexpected connector.class")
print(f"connector config OK: {name} -> {config['topic.prefix']} ({config['table.include.list']})")
PY

if [ "$DRY_RUN" -eq 1 ]; then
  echo "POST $CONNECT_URL/connectors"
  echo "  body: $CONNECTOR_FILE"
  exit 0
fi

echo "Registering connector at $CONNECT_URL ..."
STATUS="$(curl -fsS -o /tmp/connect-response.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data-binary @"$CONNECTOR_FILE" \
  "$CONNECT_URL/connectors")"

case "$STATUS" in
  200|201) echo "  connector applied (HTTP $STATUS)"; cat /tmp/connect-response.json; echo ;;
  409) echo "  connector already exists with a different config (HTTP 409) - re-run after review"; exit 1 ;;
  *) echo "  unexpected HTTP $STATUS:"; cat /tmp/connect-response.json; exit 1 ;;
esac
rm -f /tmp/connect-response.json

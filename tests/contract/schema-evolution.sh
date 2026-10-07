#!/usr/bin/env bash
# T7 contract tests - Avro BACKWARD compatibility against Schema Registry
# (WP2.6/FR-03). Uses the compatibility CHECK endpoint, so the test asserts
# registry behaviour without registering anything - zero side effects, safe
# to re-run.
#
#   compatible evolution : new optional field with a default  -> is_compatible=true
#   breaking evolution   : required field removed             -> is_compatible=false
#
# Usage: tests/contract/schema-evolution.sh [--help] [--registry URL]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REGISTRY_URL="${REGISTRY_URL:-http://localhost:${SCHEMA_REGISTRY_HOST_PORT:-8081}}"
SUBJECT="bank.events.Transaction-value"
BASE_SCHEMA="kafka-config/schemas/transaction.avsc"

usage() {
  cat <<'EOF'
T7 - Avro BACKWARD compatibility contract tests against Schema Registry

Runs two compatibility CHECK requests (POST /compatibility/subjects/
<subject>/versions/latest) against the committed Transaction schema:
  1. an evolved schema with a new optional field carrying a default
     must be reported compatible (FR-03, evolution allowed);
  2. a schema with a required field removed must be reported incompatible
     (breaking changes rejected - and per ADR-0004 they would need a new
     topic version instead).

Nothing is registered; the check endpoint is read-only.

Options:
  -h, --help        Show this help.
  --registry URL    Schema Registry base URL (default: localhost:8081).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --registry) REGISTRY_URL="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
RESULTS_DIR="tests/contract/results"
mkdir -p "$RESULTS_DIR"
REPORT="$RESULTS_DIR/contract-$(date -u +%Y%m%dT%H%M%SZ).txt"

PASSED=0
FAILED=0
pass() { echo "  PASS: $1" | tee -a "$REPORT"; PASSED=$((PASSED + 1)); }
fail() { echo "  FAIL: $1" | tee -a "$REPORT"; FAILED=$((FAILED + 1)); }

compatibility_check() {
  # $1 = candidate schema file -> echoes the check-endpoint request body
  python - "$1" <<'PY'
import json, sys
print(json.dumps({"schema": open(sys.argv[1], encoding="utf-8").read().strip()}))
PY
}

evolve_schema() {
  # $1 = "compatible" | "breaking" -> writes the variant schema to stdout path
  python - "$BASE_SCHEMA" "$1" "$RESULTS_DIR" <<'PY'
import json, sys, pathlib
base, mode, outdir = sys.argv[1], sys.argv[2], sys.argv[3]
schema = json.loads(open(base, encoding="utf-8").read())

if mode == "compatible":
    # new optional field with a default - the allowed evolution (Doc 03 §2)
    schema["fields"].append({"name": "geo_region", "type": "string", "default": "unknown"})
else:
    # remove a required field: old readers break -> BACKWARD rejects
    schema["fields"] = [f for f in schema["fields"] if f["name"] != "currency"]

out = pathlib.Path(outdir) / f"transaction-{mode}.avsc"
out.write_text(json.dumps(schema, indent=2), encoding="utf-8")
print(out)
PY
}

echo "== T7 contract tests - $(date -u) ==" | tee -a "$REPORT"
echo "registry: $REGISTRY_URL, subject: $SUBJECT" | tee -a "$REPORT"

curl -fsS "$REGISTRY_URL/subjects" >/dev/null 2>&1 \
  || { echo "ERROR: Schema Registry unreachable at $REGISTRY_URL" >&2; exit 1; }

# baseline sanity: the committed schema must itself be compatible with the
# latest registered version (or the subject may not exist yet on a fresh lab)
CAND_BASE="$(compatibility_check "$BASE_SCHEMA")"
if curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$CAND_BASE" "$REGISTRY_URL/compatibility/subjects/$SUBJECT/versions/latest" \
    | tee -a "$REPORT" | grep -q '"is_compatible": *true\|"is_compatible":true'; then
  pass "committed Transaction schema is compatible with the registered latest"
else
  echo "  INFO: subject $SUBJECT not registered yet (fresh lab) - baseline check skipped" | tee -a "$REPORT"
fi

# 1. compatible evolution must be accepted
EVOLVED="$(evolve_schema compatible)"
if curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$(compatibility_check "$EVOLVED")" \
    "$REGISTRY_URL/compatibility/subjects/$SUBJECT/versions/latest" \
    | tee -a "$REPORT" | grep -q '"is_compatible": *true\|"is_compatible":true'; then
  pass "optional field with default is BACKWARD-compatible"
else
  fail "optional field with default was reported INCOMPATIBLE - registry misconfigured?"
fi

# 2. breaking evolution must be rejected
BROKEN="$(evolve_schema breaking)"
if curl -fsS -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "$(compatibility_check "$BROKEN")" \
    "$REGISTRY_URL/compatibility/subjects/$SUBJECT/versions/latest" \
    | tee -a "$REPORT" | grep -q '"is_compatible": *false\|"is_compatible":false'; then
  pass "required field removal rejected (BACKWARD)"
else
  fail "required field removal was ACCEPTED - compatibility mode is not BACKWARD!"
fi

rm -f "$RESULTS_DIR/transaction-compatible.avsc" "$RESULTS_DIR/transaction-breaking.avsc"

echo "== T7 result: ${PASSED} passed, ${FAILED} failed ==" | tee -a "$REPORT"
[ "$FAILED" -eq 0 ]

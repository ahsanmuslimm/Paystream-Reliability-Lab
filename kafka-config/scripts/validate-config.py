#!/usr/bin/env python3
"""validate-config.py - configuration lint for PayStream Reliability Lab (T9).

Enforces the Document 03 / Document 01 standards without any third-party
dependencies (a tiny regex-based YAML subset parser is used so the script
runs identically on laptops and CI runners):

  * topic naming standard <domain>.<entity>.v<N> (plus .retry / .dlq suffixes
    and the two infrastructure prefixes pg. and connect-)
  * replication factor 3 everywhere; min.insync.replicas 2 on delete topics
  * partitions present and > 0
  * ACL rules: no wildcard principals, no ALL on a '*' resource, known
    operations only, known pattern values only
  * Avro schemas parse as JSON and carry the required record/field structure
  * broker baseline flags present in the core compose file (auto-create off,
    RF 3, min ISR 2, unclean leader election off)

Exit code 0 = conforming, 1 = violations found (violations are printed).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

TOPIC_NAME_RE = re.compile(r"^[a-z][a-z0-9-]*\.[a-z0-9._-]+\.v\d+(\.(retry|dlq))?$")
ALLOWED_TOPIC_PREFIXES = ("pg.", "connect-")
KNOWN_OPERATIONS = {"READ", "WRITE", "CREATE", "ALTER", "DELETE", "DESCRIBE", "ALL", "IDEMPOTENT_WRITE"}
KNOWN_PATTERNS = {"literal", "prefixed"}
KNOWN_RESOURCE_TYPES = {"topic", "group", "cluster", "transactional_id"}

violations: list[str] = []


def err(msg: str) -> None:
    violations.append(msg)


# --------------------------------------------------------------------------
# Minimal YAML subset parser: mappings, lists of mappings, scalars.
# Sufficient for topics.yaml and acls.yaml which use a regular layout.
# --------------------------------------------------------------------------
def parse_simple_yaml(path: Path) -> list[dict]:
    entries: list[dict] = []
    current: dict | None = None
    sub: dict | None = None
    top_key: str | None = None

    for lineno, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        line = line.replace("\t", "  ")

        m = re.match(r"^(\S[^:]*):\s*$", line)
        if m and not line.startswith(" "):
            top_key = m.group(1).strip()
            continue

        m = re.match(r"^  - ([\w.-]+): (.*)$", line)
        if m:
            current = {m.group(1): _scalar(m.group(2)), "_line": lineno}
            entries.append(current)
            sub = None
            continue

        if current is None:
            continue

        m = re.match(r"^    ([\w.-]+):\s*$", line)
        if m:
            sub = {}
            current[m.group(1)] = sub
            continue

        m = re.match(r"^    operations: \[(.*)\]$", line)
        if m:
            current["operations"] = [op.strip() for op in m.group(1).split(",") if op.strip()]
            continue

        m = re.match(r"^    ([\w.-]+): (.*)$", line)
        if m:
            current[m.group(1)] = _scalar(m.group(2))
            sub = None
            continue

        m = re.match(r"^      ([\w.-]+): (.*)$", line)
        if m and sub is not None:
            sub[m.group(1)] = _scalar(m.group(2))
            continue

    if top_key is None:
        err(f"{path.name}: missing top-level key")
    return entries


def _scalar(text: str) -> str | None:
    text = text.strip()
    if text.startswith('"'):
        end = text.find('"', 1)
        return text[1:end] if end > 0 else text[1:]
    # strip trailing inline comments on unquoted values
    if " #" in text:
        text = text.split(" #", 1)[0].strip()
    return text


# --------------------------------------------------------------------------
# checks
# --------------------------------------------------------------------------
def check_topics() -> None:
    path = REPO_ROOT / "kafka-config" / "topics.yaml"
    entries = parse_simple_yaml(path)
    if not entries:
        err("topics.yaml: no topics parsed")
        return
    seen: set[str] = set()
    for t in entries:
        name = t.get("name")
        if not name:
            err(f"topics.yaml: topic entry without a name near line {t.get('_line')}")
            continue
        if name in seen:
            err(f"topics.yaml: duplicate topic {name}")
        seen.add(name)

        if not TOPIC_NAME_RE.match(name) and not name.startswith(ALLOWED_TOPIC_PREFIXES):
            err(f"topics.yaml: {name} violates naming standard <domain>.<entity>.v<N>")

        rf = t.get("replication-factor")
        if rf != "3":
            err(f"topics.yaml: {name} replication-factor must be 3, got {rf}")

        partitions = t.get("partitions")
        if partitions is None or int(partitions) < 1:
            err(f"topics.yaml: {name} partitions must be >= 1, got {partitions}")

        cfg = t.get("config") or {}
        cleanup = str(cfg.get("cleanup.policy", "delete"))
        if cleanup == "delete" and cfg.get("min.insync.replicas") != "2":
            err(f"topics.yaml: {name} must set min.insync.replicas=2 (delete cleanup)")
        if cleanup not in ("delete", "compact", "compact,delete"):
            err(f"topics.yaml: {name} unknown cleanup.policy {cleanup}")


def check_acls() -> None:
    path = REPO_ROOT / "kafka-config" / "acls.yaml"
    entries = parse_simple_yaml(path)
    if not entries:
        err("acls.yaml: no ACLs parsed")
        return
    for a in entries:
        principal = a.get("principal", "")
        if not principal:
            err(f"acls.yaml: entry without principal near line {a.get('_line')}")
            continue
        if "*" in principal:
            err(f"acls.yaml: wildcard principal forbidden: {principal}")
        res = a.get("resource") or {}
        rname = str(res.get("name", ""))
        rtype = res.get("type", "")
        if rtype not in KNOWN_RESOURCE_TYPES:
            err(f"acls.yaml: {principal} unknown resource type {rtype}")
        if rname == "*":
            err(f"acls.yaml: {principal} uses ALL on '*' resource - forbidden")
        ops = a.get("operations", [])
        if not isinstance(ops, list) or not ops:
            err(f"acls.yaml: {principal} {rname} has no operations")
            continue
        for op in ops:
            if op not in KNOWN_OPERATIONS:
                err(f"acls.yaml: {principal} {rname} unknown operation {op}")
        if "ALL" in ops and rname == "*":
            err(f"acls.yaml: {principal} ALL on '*' - forbidden")
        pattern = res.get("pattern", "literal")
        if pattern not in KNOWN_PATTERNS:
            err(f"acls.yaml: {principal} unknown pattern {pattern}")
        # Wildcard names inside literals are also forbidden (e.g. "bank.*")
        if "*" in rname:
            err(f"acls.yaml: {principal} resource name contains '*': {rname}")


def check_schemas() -> None:
    schemas_dir = REPO_ROOT / "kafka-config" / "schemas"
    required_fields = {
        "Transaction": {"txn_id", "account_id", "amount", "currency", "type", "channel", "event_time"},
        "FraudAlert": {"alert_id", "txn_id", "account_id", "rule_name", "severity", "detected_at"},
        "Notification": {"notification_id", "customer_id", "alert_id", "channel", "created_at"},
    }
    found = set()
    for f in sorted(schemas_dir.glob("*.avsc")):
        try:
            schema = json.loads(f.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            err(f"schemas/{f.name}: invalid JSON: {e}")
            continue
        rec = schema.get("name")
        found.add(rec)
        fields = {fld.get("name") for fld in schema.get("fields", [])}
        missing = required_fields.get(rec, set()) - fields
        if missing:
            err(f"schemas/{f.name}: missing required fields {sorted(missing)}")
        for fld in schema.get("fields", []):
            if "default" not in fld and "null" in json.dumps(fld.get("type")):
                # union with null must carry a default for BACKWARD compatibility
                err(f"schemas/{f.name}: field {fld.get('name')} is a null union without default")
    missing_files = set(required_fields) - found
    if missing_files:
        err(f"schemas/: missing schema records {sorted(missing_files)}")


def check_broker_baselines() -> None:
    compose = REPO_ROOT / "infra" / "compose" / "docker-compose.yml"
    if not compose.exists():
        err("infra/compose/docker-compose.yml: not found")
        return
    text = compose.read_text(encoding="utf-8")
    required = {
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE": '"false"',
        "KAFKA_DEFAULT_REPLICATION_FACTOR": "3",
        "KAFKA_UNCLEAN_LEADER_ELECTION_ENABLE": '"false"',
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "3",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "2",
    }
    for key, expected in required.items():
        if f"{key}: {expected}" not in text:
            err(f"docker-compose.yml: broker baseline {key}: {expected} missing")


def main() -> int:
    check_topics()
    check_acls()
    check_schemas()
    check_broker_baselines()
    if violations:
        print(f"validate-config: {len(violations)} violation(s):")
        for v in violations:
            print(f"  - {v}")
        return 1
    print("validate-config: all configuration conforms to the standards")
    return 0


if __name__ == "__main__":
    sys.exit(main())

# Stage 2 gate (G2) readiness checklist

Date: 2026-10-07. Maps the deliverables register and G2 exit criteria
(implementation plan §4.3 / §8.3) to their artifacts. Honest statuses:
**ready** = artifact exists and is verified at authoring level; **pending
execution** = artifact exists, the criterion needs the running cluster.

Gate date: **15 Nov 2026**. Everything listed "pending execution" needs
Docker Desktop on the reference workstation (currently not installed).

## Deliverables register

| Area | Artifact | Status |
|---|---|---|
| docs/runbooks/ | 8 runbooks with Detect/Diagnose/Mitigate/Recover/Prevent | ready; *exercise during drills* pending execution |
| docs/drills/ | 6 reports from captured evidence | pending execution (chaos/d1–d6 + results/ ready) |
| docs/adr/ | ADR-0001 … ADR-0006 | ready (0005, 0006 added 2026-10-07) |
| docs/performance/ | baseline.md scaffold, staged-load.sh, soak.sh | pending execution (T10/T14 numbers) |
| docs/security/ | threat-model.md, acl-matrix.md | ready; T8 agreement check pending execution |
| docs/operations/ | deployment-guide.md (incl. secure mode), config-standards.md | ready |
| kafka-config/ | topics.yaml, acls.yaml, 3 .avsc, connector JSON, validate-config.py, apply-*.sh, check-drift.sh | ready; validate-config green; drift/apply on cluster pending execution |
| security/scripts/ | gen-ca, gen-broker-cert, gen-client-cert, rotate-certs, create-scram-users, bootstrap-security | ready (chain + expiry negative tests verified locally) |
| services/ | parent POM + common-avro, txn-producer, fraud-detector, notifier, dlq-replay; Dockerfiles | ready (45 tests green incl. EmbeddedKafka T6 wiring test) |
| monitoring/ | dashboards ×3, kafka-alerts.yml, alertmanager, loki/promtail, jmx/broker.yml, pushgateway cert exporter | ready; *alerts firing on cluster* pending execution |
| chaos/ | d1–d6 scripts, lib.sh, evidence capture | ready; executed-twice rule pending execution |
| tests/ | integration (Testcontainers IT), t8-security-tests.sh, schema-evolution.sh, staged-load.sh, soak.sh, rolling-restart.sh, reassign-partitions.sh + expansion overlay | ready; cluster suites pending execution |
| workflows/ | ci.yml, config-validate.yml, release.yml | ready |
| Makefile + scripts/ | up/down/up-secure/certs/scram/acls/topics/contract/t8-security/drift/replay-dlq/cert-expiry-check/drill-d1..6; bootstrap, wait-for-cluster, smoke-test, cert-expiry-check, collect-diagnostics | ready; G1 cold-start rehearsal pending execution |

## G2 exit criteria (all mandatory, 15 Nov 2026)

| # | Criterion | Path | Status |
|---|---|---|---|
| 1 | Six of six drills documented with RCA + runbook, each executed twice | chaos/d*.sh → docs/drills/d*.md | pending execution |
| 2 | Zero lost, zero duplicated messages in the D1 30-minute run | drill D1 evidence + notifier idempotency tests | pending execution |
| 3 | Rolling upgrade with zero producer errors | tests/upgrade/rolling-restart.sh | pending execution |
| 4 | CI green on main including security and lint stages | .github/workflows/ci.yml | ready (push to verify) |
| 5 | Performance baseline published with hardware stated | docs/performance/baseline.md | pending execution |
| 6 | Reviewer reproduces D1 from documentation alone | docs/runbooks/broker-down.md + chaos/d1_broker_loss.sh | ready for review; dry-run the walkthrough when Docker is back |

## Known calibration risks for the first secured run

One pass of `make up-secure` is budgeted to calibrate: controller-listener
SSL config keys on Apache Kafka 4.0, kafka-exporter SASL flags, Schema
Registry SASL env names, and Connect-over-SASL_SSL. These are written per
ADR-0002 and the docs but can only be exercised on a running cluster.

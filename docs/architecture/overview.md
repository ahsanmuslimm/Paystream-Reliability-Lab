# Architecture overview

Production-style event streaming platform for a mock retail bank. This page is
the 5-minute tour; deep detail lives in the ADRs and the source documents in
`Documents/`.

## Components and data flow

```
txn-producer ─────────► bank.transactions.v1 (6 partitions, RF=3, min ISR=2)
 (synthetic workload,      │
  keyed by account_id)     ▼
                      fraud-detector (Kafka Streams, app id: fraud-detector)
                       · amount rule: amount > threshold → FraudAlert
                       · velocity rule: N txns per account in a 60 s
                         window → one VELOCITY_WINDOW alert per burst
                          │   poison records → bank.transactions.v1.dlq
                          ▼   (Document 03 header set, ADR-0006)
                      bank.fraud-alerts.v1 (3 partitions)
                          │
                          ▼
                      notifier (idempotent consumer, group: notifier)
                       · processed_events marker + business rows in ONE tx
                       · Kafka offset committed after the DB commit
                       · poison alerts → bank.fraud-alerts.v1.dlq;
                         replay via services/dlq-replay (dlq-replay.md)
                          │
                          ├─► PostgreSQL: accounts, processed_events,
                          │   fraud_alerts, notifications, ops tables
                          └─► bank.notifications.v1 (downstream demo)
```

All topic definitions live in `kafka-config/topics.yaml` (source of truth);
all Avro schemas in `kafka-config/schemas/` — the `common-avro` Maven module
generates classes from the very same files, so registry and code can never
drift.

## Trust boundaries

| Zone | Contents | MVP controls | Stage 2 controls |
|---|---|---|---|
| Compose bridge network | brokers, SR, Postgres, services | isolated bridge network, no host exposure beyond mapped ports | TLS/mTLS + SASL/SCRAM on every listener, default-deny ACLs |
| Host laptop | ports 3000/8080/8081/8082/8083/9090/5432/1909x | Grafana/Prometheus auth, no secrets in Git | SCRAM credentials, PKI, secret hygiene (Gitleaks gate) |
| CI (GitHub Actions) | build, tests, config lint, scans | read-only checkout + PR gates | image signing, registry credentials via OIDC |

No component is exposed to the public internet by the Compose files; every
published port binds to the workstation loopback by default (Docker Desktop
default behaviour).

## Failure domains and the six drills

| # | Drill | Failure domain exercised | First responder |
|---|---|---|---|
| D1 | broker loss | leader election, client retry, ISR catch-up | `docs/runbooks/broker-down.md` |
| D2 | ISR shrink | min.insync.replicas write unavailability | `docs/runbooks/isr-shrink.md` |
| D3 | lag storm | consumer starvation, rebalance thrash | `docs/runbooks/consumer-lag.md` |
| D4 | disk full | log volume exhaustion | `docs/runbooks/disk-full.md` |
| D5 | cert expiry | TLS handshake failure | `docs/runbooks/cert-rotation.md` |
| D6 | poison message | deserialization failure | `docs/runbooks/dlq-replay.md` |

## Design decisions index

| ADR | Decision |
|---|---|
| [0001](0001-cluster-topology.md) | 3 combined broker+controller nodes for the lab |
| [0002](0002-listener-security-layout.md) | listener layout pre-wired for mTLS/SCRAM |
| [0003](0003-topic-partitions-keying.md) | 6 partitions, keyed by account_id |
| [0004](0004-schema-compatibility.md) | Avro BACKWARD, TopicNameStrategy, schemas as code |
| [0005](0005-cooperative-rebalancing.md) | CooperativeStickyAssignor everywhere (D3 support) |
| [0006](0006-dlq-envelope-replay.md) | DLQ envelope (Document 03 headers), retry semantics, replay tool |

## Positioning

Production-style, not production-ready: production *practices* (security,
HA, observability, runbooks, drills) on laptop hardware with synthetic data.
The full limitations list is maintained in the repository README and reviewed
at every stage gate.

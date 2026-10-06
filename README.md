# PayStream Reliability Lab

A production-style Apache Kafka streaming platform running a mock retail-banking
event workload — three KRaft brokers, Schema Registry, three Java 21 / Spring
Boot services, monitoring, and a documented failure-drill programme with
runbooks.

> **Positioning note.** Everything here is *production-style*: it follows
> production practices (security, HA, observability, runbooks) on laptop-class
> hardware with synthetic data. It is not certified and has not carried real
> production traffic. See [Known limitations](#known-limitations).

## Architecture

```
                       ┌──────────────────────────────────────────────┐
                       │          Kafka cluster (KRaft, RF=3)         │
 txn-producer ────────►│  bank.transactions.v1 (6p, keyed account_id) │
 (Spring Boot,         │            │                                 │
  1–5,000 msg/s)       │            ▼                                 │
                       │  fraud-detector (Kafka Streams)              │
                       │   amount rule → bank.fraud-alerts.v1         │
                       │            │                                 │
                       │            ▼                                 │
                       │  notifier (idempotent consumer)              │
                       │   → PostgreSQL (alerts + notifications)      │
                       │   → bank.notifications.v1                    │
                       └──────────────────────────────────────────────┘
                              │                │
                       kafka-exporter    /actuator/prometheus
                              ▼                ▼
                       Prometheus → Grafana (dashboards + alert rules)
```

| Component | Choice | Pinned version |
|---|---|---|
| Event streaming | Apache Kafka, KRaft combined mode (ADR-0001) | `apache/kafka:4.0.0` |
| Schema management | Confluent Schema Registry (community licence), Avro BACKWARD (ADR-0004) | `cp-schema-registry:7.9.0` |
| Services | Java 21 LTS, Spring Boot 3.5.7, Spring Kafka, Kafka Streams | — |
| Persistence | PostgreSQL 16 + Flyway migrations V1–V4 | `postgres:16-alpine` |
| Observability | kafka-exporter → Prometheus → Grafana | `v1.10.0` / `v3.4.1` / `12.0.0` |

## Quickstart

Prerequisites: Docker + Compose v2, JDK 21, Maven 3.9+ (build only).

```bash
cp .env.example .env      # pinned versions and non-secret defaults
make up                   # images, stack, topics, ready
make smoke                # T2: verifies producer → alert → notification row
```

Then:

- **Grafana** — http://localhost:3000 (admin/admin) → *PayStream* folder →
  *Cluster Overview (MVP)*
- **Prometheus** — http://localhost:9090
- **Producer API** — `GET http://localhost:8080/api/generation/status`
  (start/stop via `POST /api/generation/start?rate=N`, 1–5,000 msg/s)

Teardown: `make down` (keeps volumes) or `make nuke` (full cold-start
rehearsal). The smoke test plus all operational scripts are idempotent and
support `--help`.

## Repository layout

One responsibility per directory (Document 04):

| Directory | Purpose |
|---|---|
| `kafka-config/` | Topics, ACLs, Avro schemas as code + validation/apply scripts |
| `services/` | Maven multi-module: `common-avro`, `txn-producer`, `fraud-detector`, `notifier` |
| `infra/compose/` | Core stack + monitoring overlay (security/connect overlays arrive in Stage 2) |
| `monitoring/` | Prometheus config + rules, Grafana provisioning + dashboards |
| `docs/` | ADRs, runbooks, drill reports, deployment guide, performance baseline |
| `chaos/` | Failure-drill scripts D1–D6 + timestamped evidence (Stage 2) |
| `scripts/` | Bootstrap, readiness, smoke test |
| `.github/workflows/` | CI: build/tests, config lint, shell/Dockerfile lint, secret scan |

## Development

```bash
make build          # compile + full unit-test suite (no Docker needed)
make validate-config# lint topics/ACLs/schemas against the Document 03 standards
```

- `common-avro` generates classes straight from `kafka-config/schemas/*.avsc` —
  the single source of truth shared with Schema Registry.
- Integration tests use Testcontainers and run wherever Docker is available
  (CI runners; locally they self-skip via `disabledWithoutDocker`).
- Idempotency: `notifier` inserts a `processed_events` marker and the business
  rows in one transaction, then commits the Kafka offset (FR-06). Replaying an
  alert any number of times yields exactly one notification row.

## Lifecycle stage status

| Stage | Scope | Status |
|---|---|---|
| 0 — Prototype | Environment baseline (P0) recorded in `.env.example` heaps | ✅ recorded 2026-10-06 |
| 1 — MVP | This slice: cluster, 3 services, amount rule, minimal dashboards, CI | 🚧 in progress |
| 2 — Final | mTLS/SCRAM/ACLs, velocity rule, DLQ, CDC, alerts, drills D1–D6, upgrade | ⬜ planned |
| 3 — Commercial-ready | K8s, IaC, DR, SLOs (optional, time-boxed) | ⬜ planned |

## Known limitations

Honest list, reviewed at every stage gate (G2 requires it):

- Transport is plaintext inside the Compose network for now; TLS/mTLS,
  SASL/SCRAM and default-deny ACLs are Stage 2 (WP2.1/WP2.2) and will be
  enforced with scripted negative tests.
- Only the amount-threshold fraud rule is implemented; the 60 s velocity rule
  arrives with the Kafka Streams hardening package (WP2.3).
- Poison records are logged-and-skipped; the DLQ envelope and replay tooling
  are Stage 2 (WP2.3).
- No Kafka Connect / Debezium CDC yet (Stage 2, WP2.4) — `pg.public.accounts`
  and Connect internal topics are pre-created but unused.
- Non-functional targets (p99 < 200 ms at 1,000 msg/s; ≥ 5,000 msg/s ceiling)
  are design targets until measured into `docs/performance/baseline.md`.
- JMX broker metrics, Alertmanager routing and Loki are Stage 2 (WP2.5); the
  MVP dashboard covers broker count, URP, lag and service metrics.

## License

MIT — see [LICENSE](LICENSE).

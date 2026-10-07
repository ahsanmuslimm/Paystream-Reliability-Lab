# Deployment guide

Bringing the PayStream Reliability Lab up from nothing (Reproducibility NFR:
cold start to working platform in one command).

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker + Compose v2 | the only runtime dependency |
| JDK 21 (Temurin) | for `make build` / local test runs |
| Maven 3.9+ | for `make build`; or use your IDE's bundled Maven |
| GNU Make | every `make <target>` has an equivalent direct command (the target bodies are the source of truth); on Windows without Make, run them from Git Bash |
| 16 GB RAM minimum (32 GB preferred) | WP0.1 baseline: broker heap 1 GB × 3, Schema Registry 512 MB, services 512 MB each |
| ~15 GB free disk | broker volumes, images, Grafana/Prometheus data |

## Cold start

```bash
git clone <repository-url> paystream-reliability-lab
cd paystream-reliability-lab
cp .env.example .env          # pin versions; adjust ports if taken
make up                       # images → 3 KRaft brokers + Schema Registry + Postgres → services → topics
bash scripts/wait-for-cluster.sh   # (already run by `make up`)
make smoke                    # T2: producer → alert → notification row
```

`make up` is idempotent: re-running it on a live stack re-applies topics
(`--if-not-exists`) and restarts nothing that is already healthy.

`make nuke` destroys the stack **and all volumes** — use it to rehearse a
genuine cold start.

## Verification checklist (MVP gate G1)

1. `docker compose -f infra/compose/docker-compose.yml ps` — all containers
   healthy (`kafka-1..3`, `schema-registry`, `postgres`, three services).
2. Quorum: `docker compose exec kafka-1 kafka-metadata-quorum.sh
   --bootstrap-server localhost:29092 describe --status` — leader present.
3. `make smoke` exits with `SMOKE TEST PASSED`.
4. Grafana at http://localhost:3000 shows *Cluster Overview (MVP)* with
   broker count 3 and moving produce-rate series.
5. CI green on the commit you checked out.

## Component endpoints

| Service | Host port | Purpose |
|---|---|---|
| Grafana | 3000 | dashboards (provisioned) |
| Prometheus | 9090 | metrics + alert rules |
| Schema Registry | 8081 | REST API |
| txn-producer | 8080 | generation control API + actuator |
| fraud-detector | 8082 | actuator |
| notifier | 8083 | actuator |
| PostgreSQL | 5432 | paystream database |
| Kafka (host) | 19091–19093 | per-broker host listener |

## Workload control

```bash
curl -X POST 'http://localhost:8080/api/generation/start?rate=1000'  # 1–5,000
curl -X POST 'http://localhost:8080/api/generation/stop'
curl http://localhost:8080/api/generation/status
```

## Troubleshooting

| Symptom | First checks |
|---|---|
| Broker container restarting | `docker logs kafka-1` — usually a stale volume after CLUSTER_ID change; `make nuke` resets |
| Schema Registry unhealthy | brokers must be healthy first; check `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` reachability |
| No notification rows growing | `make logs`; confirm fraud-detector Streams state RUNNING and notifier consumer group lag shrinking |
| Port conflicts | edit host ports in `.env`, re-run `make up` |

## Upgrade note

Version pins live in `.env` and `services/pom.xml`. Changing the Kafka image
version requires `make nuke` unless the upgrade procedure
(`docs/runbooks/rolling-upgrade.md`, Stage 2) is being followed — that runbook
performs a rolling restart instead, preserving data.

## Secured mode (Stage 2, `make up-secure`)

The secured stack switches every listener to TLS per ADR-0002: mTLS between
brokers and on the KRaft controller listener, SASL_SSL with SCRAM-SHA-512 for
clients, default-deny StandardAuthorizer with the Document 03 ACL matrix.

```bash
make certs        # CA + 3 broker certs + kafka-setup/svc-demo client certs (idempotent)
make scram        # random SCRAM credentials + per-service JAAS env files (git-ignored)
make up-secure    # core + monitoring + security overlay; kafka-setup bootstraps users/ACLs/topics
```

What `up-secure` does on first start:

1. Brokers come up with SSL/SASL_SSL listeners; the healthcheck authenticates
   as the broker's own certificate (CN=broker-N, superuser).
2. The one-shot `kafka-setup` container registers all SCRAM users (as
   `User:CN=kafka-setup`), then applies `acls.yaml` and `topics.yaml` as
   `svc-admin-ci`.
3. Schema Registry and the services start only after setup completed
   (`service_completed_successfully`) and connect with their own principals.

Host-side admin tooling authenticates over `localhost:19091` with
`security/secrets/admin.properties` (svc-admin-ci); the mTLS-only BROKER
listener is exposed on `localhost:19094` (demo client, bootstrap identity).

| Endpoint | Host port | Auth |
|---|---|---|
| Kafka client listener (SASL_SSL) | 19091–19093 | SCRAM-SHA-512 |
| Kafka broker mTLS listener | 19094 | client certificate |

Verify: `make drift` (config matches cluster), `make acls` (idempotent
re-apply), `bash security/scripts/rotate-certs.sh --check` (cert inventory),
`make cert-expiry-check` (pushes expiry metrics; feeds the D5 alerts).

### Secure CDC (Connect on the secured cluster)

`make up-connect` runs Connect in plaintext against the MVP stack. For the
secured cluster, export before `up-connect`:

```bash
export CONNECT_BOOTSTRAP="SASL_SSL://kafka-1:29092,SASL_SSL://kafka-2:29092,SASL_SSL://kafka-3:29092"
export CONNECT_SECURITY_PROTOCOL="SASL_SSL"
export CONNECT_SASL_MECHANISM="SCRAM-SHA-512"
export CONNECT_SASL_JAAS_CONFIG="$(grep svc-connect security/secrets/scram-credentials.env | cut -d= -f2- | xargs -I{} echo 'org.apache.kafka.common.security.scram.ScramLoginModule required username="svc-connect" password="{}";')"
# plus the truststore wiring (ssl.truststore.*) from the runbook; runtime
# calibration of Connect-over-SASL_SSL happens with Docker available
```

### Broker JMX exporter (optional WP2.5 activation)

Mount `monitoring/jmx/broker.yml` and add
`-javaagent:/opt/jmx_exporter/jmx_prometheus_javaagent.jar=7071:/etc/kafka/jmx/broker.yml`
to the broker JVM args (e.g. via KAFKA_EXTRA_ARGS), then uncomment the
`kafka-broker-jmx` scrape job in `monitoring/prometheus/prometheus.yml`.
The `KafkaOfflinePartitions` / `NoActiveController` alerts activate with it.

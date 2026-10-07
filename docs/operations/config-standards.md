# Configuration standards

What every configuration change in this repository must look like. Enforced
by `kafka-config/scripts/validate-config.py` (CI-blocking, T9).

## Topics

- Naming: `<domain>.<entity>.v<N>` — lowercase, dot-separated, version
  suffix; `.retry` and `.dlq` suffixes for the retry/DLQ pattern.
- Replication factor **3** everywhere; `min.insync.replicas=2` on every
  delete-cleanup topic.
- Auto-creation disabled on brokers (`auto.create.topics.enable=false`);
  the single source of truth is `kafka-config/topics.yaml`.
- Infrastructure prefixes `pg.` (Debezium CDC) and `connect-` (Connect
  internal topics) are reserved; do not use them for business topics.

## Schemas

- Avro, BACKWARD compatibility, TopicNameStrategy (ADR-0004).
- One `.avsc` file per record in `kafka-config/schemas/`; null unions must
  carry defaults; no renames or type changes (new field + deprecate, or new
  topic version).
- Java classes are generated from those files in the `common-avro` module —
  never hand-write or copy schema classes.

## ACLs

- `kafka-config/acls.yaml` is the only source of truth.
- No wildcard principals (`User:*`), no `ALL` on `*` resources, no wildcard
  resource names.
- One principal per service identity (`svc-txn-producer`, `svc-fraud`,
  `svc-notify`, `svc-connect`, `svc-admin-ci`); humans never use service
  credentials.
- Default deny (`allow.everyone.if.no.acl.found=false`); every non-granted
  operation must be denied (negative tests, T8).

## Broker baselines (Document 03 §5.1)

`auto.create.topics.enable=false`, `default.replication.factor=3`,
`min.insync.replicas=2`, `unclean.leader.election.enable=false`,
`offsets.topic.replication.factor=3`,
`transaction.state.log.replication.factor=3`, `transaction.state.log.min.isr=2`,
`log.retention.hours=168`, `delete.topic.enable=true`.

Present in `infra/compose/docker-compose.yml` and verified by
validate-config.py; the Stage 2 security overlay adds
`StandardAuthorizer` + `allow.everyone.if.no.acl.found=false`.

## Producer baselines (Document 03 §5.2)

`acks=all`, `enable.idempotence=true`, `compression.type=lz4`,
`linger.ms=10`, `delivery.timeout.ms=120000`.

## Consumer baselines (Document 03 §5.3)

`enable.auto.commit=false`, `auto.offset.reset=earliest`,
`session.timeout.ms=45000`, `max.poll.interval.ms=300000`,
`partition.assignment.strategy=CooperativeStickyAssignor`.

## Security additions (Stage 2, ADR-0006)

The notifier DLQ/retry topics, `_schemas`, the `svc-monitor` read-only
metrics principal and the `connect-cluster` group are ADR-0006 additions to
the Document 03 matrix - recorded there under change control, mirrored in
`kafka-config/`, and enforced by validate-config. No wildcard principals
were introduced.

## Change process

1. Edit the YAML/avsc file (never change a running system directly).
2. `make validate-config` must pass.
3. Pull request → CI applies the same validation; merge to `main` is the
   deploy trigger (drift check arrives in Stage 2, WP2.6).
4. Record the change in the `ops_change_log` table (TOPIC, ACL, UPGRADE,
   CERT_ROTATION) with the git commit.

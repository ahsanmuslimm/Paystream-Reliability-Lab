# ADR-0006: Dead-letter envelope, retry semantics and replay procedure

Status: accepted (2026-10-07)
Drivers: WP2.3 stream hardening, FR-05/FR-06/FR-07, drill D6

## Context

Stage 1 logs and skips poison records. Stage 2 must dead-letter them (FR-07)
while keeping the consumer alive, and Document 03 section 3 fixes the DLQ
value as the original raw bytes plus a defined record-header set:

| Header | Meaning |
|---|---|
| dlq.original.topic | Source topic |
| dlq.original.partition / dlq.original.offset | Source position |
| dlq.error.class | Exception class |
| dlq.error.message | Truncated message (512 chars, flattened to one line) |
| dlq.failed.at | Timestamp (ISO-8601 UTC) |
| dlq.consumer.group | Failing consumer group |
| dlq.attempts | Number of attempts made |

## Decision

1. **One header set everywhere.** All three producers of DLQ records - the
   fraud-detector Streams deserialization handler, the Streams processing
   handler, and the notifier's `DlqRecoverer` - emit exactly the Document 03
   headers above. Spring's default `kafka_original*` headers are not used.

2. **Streams side (fraud-detector).** `DlqDeserializationExceptionHandler`
   and `DlqProcessingExceptionHandler` publish raw bytes to
   `bank.transactions.v1.dlq` and return CONTINUE, so the Streams thread
   keeps running. If the DLQ publish itself fails they return FAIL - a
   record we could not even dead-letter must not be silently dropped.

3. **Consumer side (notifier).** The value (and key) deserializers are wrapped
   in Spring's `ErrorHandlingDeserializer` - with kafka-clients 3.9 (KIP-899)
   the consumer cannot hand a failed record to the error handler itself, so
   the wrapper carries the raw failed bytes to the recoverer on the
   `DeserializationException`. `DefaultErrorHandler` classifies
   deserialization failures as non-retryable (poison is never fixed by
   retrying) and recovers them to `bank.fraud-alerts.v1.dlq` immediately.
   Other exceptions get two backoff retries (1 s apart); after the retries
   are exhausted a record that has no raw payload (it was already
   deserialized) is logged at ERROR and skipped rather than dead-lettered -
   dead-lettering it would violate the raw-bytes envelope. The lag and
   error alerts cover that path. Verified by the EmbeddedKafka wiring test
   `NotifierDlqWiringTest` (container-free T6 precursor); the D6 drill
   re-proves it end to end on the cluster.

4. **Retry topics.** `bank.transactions.v1.retry` and
   `bank.fraud-alerts.v1.retry` are declared topics that serve as the
   operator's deferred reprocessing entry point (replay tool `--target`),
   not as an automated intermediate hop. In-memory backoff + DLQ is the
   automated path; topic-based retry chains are a Stage 3 candidate if the
   operational need appears.

5. **Replay procedure.** `services/dlq-replay` re-publishes the preserved
   raw bytes (and key) back to `dlq.original.topic`, or to `--target`
   (typically the `.retry` topic), commits the DLQ offset per record, and
   reports replayed/skipped/failed counts. Runbook:
   `docs/runbooks/dlq-replay.md`. The `dlq-replay` group id is deliberately
   excluded from the notifier's idempotency logic - replay is a re-entry
   into normal processing, where the `processed_events` marker collapses
   duplicates.

## Change-control note (plan section 11.3)

The Document 03 ACL matrix and topic catalogue list `.retry`/`.dlq` only for
the transactions domain. The notifier consumes the fraud-alerts domain and
needs its own DLQ (FR-07 applies to every consumer). Applied additions,
each mirrored in `kafka-config/topics.yaml` / `acls.yaml` and validated by
CI:

- topics `bank.fraud-alerts.v1.retry`, `bank.fraud-alerts.v1.dlq`;
  `_schemas` (Schema Registry manages it itself, and broker auto-creation
  is disabled);
- ACLs: `svc-notify` on the alerts retry/DLQ topics (mirroring `svc-fraud`);
  `svc-schema-registry` on `_schemas` + cluster DESCRIBE; `svc-monitor`
  (kafka-exporter) with cluster DESCRIBE/DESCRIBE_CONFIGS and prefixed
  DESCRIBE grants; `svc-connect` group READ on `connect-cluster`.

No wildcard principals were introduced; all grants stay enumerable.

## Consequences

- D6 exercises one coherent poison path end to end: malformed record ->
  DLQ with the documented headers -> alert `DlqBacklog` -> replay tool ->
  duplicates collapsed by idempotency.
- The DLQ value remains byte-exact, so schema re-interpretation after a
  bugfix is always possible.
- Spring's `DeadLetterPublishingRecoverer` was consciously not reused: its
  header names differ from Document 03 and its Avro handling would
  re-serialize rather than preserve raw bytes.

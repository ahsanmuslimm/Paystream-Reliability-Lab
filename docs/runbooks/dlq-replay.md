# Runbook: DLQ replay (D6)

Symptom: poison records sit in a `.dlq` topic; `DlqBacklog` alert fires after 30 min.

## Detect
- Alert `DlqBacklog` (warning) - any `.dlq` topic non-empty for 30 min.
- Notifier/fraud-detector logs: "moved to DLQ" lines carry the failure reason.

## Diagnose
- Inspect headers of a DLQ record (dlq.original.topic, dlq.error.class, dlq.error.message, dlq.attempts) with kafka-console-consumer --property print.headers=true.
- Decide: fix-forward (replay after a code fix) or discard (record is genuinely invalid).

## Mitigate / replay
- Dry run first (counts what would move, produces nothing):
  `make replay-dlq ARGS="--dlq bank.transactions.v1.dlq --dry-run --limit 100"`
- Replay to the original topic (or the .retry topic for deferred processing):
  `make replay-dlq ARGS="--dlq bank.transactions.v1.dlq --target bank.transactions.v1.retry"`
- The tool preserves the raw bytes and key (Document 03 envelope), commits the DLQ offset per record, and prints replayed/skipped/failed.

## Recover
- On the secured cluster, replay as svc-fraud (transactions DLQ) or svc-notify (alerts DLQ) via `--config <client properties>`.

## Prevent
- Duplicate replays are collapsed by the notifier's processed_events marker (FR-06); the fraud-detector is deterministic (ADR-0006).
- D6 drill executed twice with evidence; see docs/drills/d6-poison-message.md.

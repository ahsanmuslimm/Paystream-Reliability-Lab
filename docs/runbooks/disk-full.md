# Runbook: Disk exhaustion (D4)

Symptom: broker log volume fills; brokers crash or reject writes.

## Detect
- Alert `NodeDiskAlmostFull` (critical, > 70% for 5m) from node-exporter.

## Diagnose
- `docker system df -v` and `du -sh /var/lib/docker/volumes/paystream_*`.
- Which topic grows? `docker compose exec kafka-1 kafka-log-dirs.sh --bootstrap-server localhost:29094 --describe` (plaintext mode) - retention outliers show up.

## Mitigate
- Stop the source rate (txn-producer) to cap growth.
- Reduce retention for the offending topic (temporary, via kafka-configs as admin) - never delete segments by hand.

## Recover
- Let retention reclaim space after growth stops; confirm node disk falls below 70% and the alert clears.

## Prevent
- log.retention.hours=168 bounds normal growth; WP2.9 soak test watches drift.
- D4 drill executed twice with evidence; see docs/drills/d4-disk-full.md.

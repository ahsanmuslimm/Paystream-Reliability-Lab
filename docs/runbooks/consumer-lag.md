# Runbook: Consumer lag and rebalance storm (D3)

Symptom: consumer group lag grows; members flap; throughput collapses.

## Detect
- Alert `ConsumerLagHigh` (warning, group lag > 1000 for 5m) or `NotifierRecordsLag`.
- Grafana consumer-groups dashboard: members per group oscillating = rebalance storm.

## Diagnose
- Which group? `sum by (consumergroup) (kafka_consumergroup_lag)`.
- Consumer logs for repeated `Attempt to heartbeat failed` / rebalance cycles.
- Classic cause: slow processing exceeding max.poll.interval.ms (300 s here) - the member is evicted, rejoins, and the cycle repeats.

## Mitigate
- The platform uses CooperativeStickyAssignor (ADR-0005): only affected partitions are revoked, so storms self-limit.
- Reduce source rate: `POST /generation/stop` on txn-producer, or lower TXN_PRODUCER_RATE_PER_SECOND.

## Recover
- Fix the slow consumer (scale up, or reduce processing cost), then watch lag drain on the dashboard.

## Prevent
- Keep processing time well under max.poll.interval.ms; load-test with WP2.9 rates.
- D3 drill executed twice with evidence; see docs/drills/d3-consumer-lag.md.

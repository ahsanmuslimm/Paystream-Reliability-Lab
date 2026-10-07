# Runbook: ISR shrink / quorum loss (D2)

Symptom: replicas drop out of sync, or two brokers are down and writes block.

## Detect
- Alert `KafkaPartitionIsrBelowMinIsr` (warning) and `KafkaBrokerCountBelowExpected` (critical).
- Producers report `NOT_ENOUGH_REPLICAS` - this is acks=all + min.insync.replicas=2 working as designed.

## Diagnose
- `make ps` - how many brokers are down? One down = shrink; two = write outage by design.
- Check broker logs for the failing replica (disk, GC, network).

## Mitigate
- Do NOT lower min.insync.replicas and do NOT enable unclean leader election - availability is sacrificed for durability deliberately.
- Producers see blocking/retries; consumers on remaining leaders continue.

## Recover
- Restart brokers one at a time; confirm ISR returns to 3 before restarting the next.
- `unclean.leader.election.enable=false` guarantees no data loss even if a stale replica claims leadership.

## Prevent
- Capacity baseline (WP0.1) keeps heaps inside safe margins.
- D2 drill executed twice with evidence; see docs/drills/d2-isr-shrink.md.

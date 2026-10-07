# ADR-0005: Consumer rebalancing with CooperativeStickyAssignor

Status: accepted (2026-10-07, retro-dated to the Stage 1 integration findings)
Drivers: D3 (consumer lag / rebalance storm), FR-06

## Context

Every consumer in the platform (the notifier's `@KafkaListener`, the Kafka
Connect workers) and every Kafka Streams instance participates in group
rebalances. The JVM client's historical default, the eager `RangeAssignor`,
revokes all partitions on every rebalance and stops the world: during drill
D3-style rebalance storms the notifier would repeatedly give up its entire
assignment, pause consumption, and re-fetch from scratch - exactly the lag
amplification D3 is designed to expose.

## Decision

All consumer groups use `partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor`:

- the notifier consumer (Document 03 section 5.3 baseline, already set);
- the fraud-detector Streams application (Streams uses the same assignor
  through its consumer properties);
- the Connect worker (`connect-cluster` group).

Cooperative (incremental) rebalancing revokes only the partitions that must
move, and the sticky half keeps otherwise-unchanged assignments stable.

## Consequences

- D3 demonstrates the fix, not just the failure: lag recovers without
  stop-the-world revoke storms, and the consumer-groups dashboard shows
  stable member counts across rebalances.
- A cooperative rebalance can produce a second short rebalance round when
  partitions must move; this is expected and bounded.
- Mixing eager and cooperative consumers in one group is forbidden - a
  lab rule reinforced by validate-config and the deployment guide.

## Alternatives considered

- `RangeAssignor`/eager rebalancing: simplest, but stop-the-world under
  exactly the failure mode the lab documents.
- Manual partition assignment: not compatible with multi-instance scaling
  drills.

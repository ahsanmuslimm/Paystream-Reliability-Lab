# Runbook: Fourth-broker expansion (T13)

Symptom-triggered: planned procedure to add capacity.

## Detect (preconditions)
- Cluster healthy (all ISR=3); expansion is a Should-item executed after T12.

## Diagnose (before)
- Record the before-state: per-broker leader counts and throughput from the cluster dashboard (the "after" comparison for FR-14).

## Mitigate (the procedure)
1. Start kafka-4 (clone of the broker service in the expansion compose variant with node.id=4, joining the same quorum voters as observer).
2. Generate the reassignment plan for all bank.* topics across 4 brokers:
   `kafka-reassign-partitions.sh --bootstrap-server ... --topics-to-move-json-file topics.json --broker-list 1,2,3,4 --generate`
3. Execute, then verify with `--verify` until every partition shows completed.

## Acceptance
- Load distribution measured before and after; no under-replicated window beyond the reassignment itself; producers keep acks=all semantics.

## Prevent
- Reassignment throttles (leader.replication.throttled.rate) protect client traffic on larger clusters - the lab records the measured impact instead.

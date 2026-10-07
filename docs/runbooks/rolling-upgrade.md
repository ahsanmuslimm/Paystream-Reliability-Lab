# Runbook: Rolling restart and rolling upgrade (T12)

Symptom-triggered: planned procedure, executed under continuous load.

## Detect (preconditions)
- All partitions ISR=3; no firing alerts; clean working tree; images for the new version built.

## Diagnose
- For version upgrades, confirm inter-broker protocol compatibility: with a one-version step and Apache Kafka 4.0, the existing log format needs no migration.

## Mitigate (the procedure)
1. One broker at a time: `docker compose stop kafka-1 && docker compose up -d kafka-1` (new image tag if upgrading).
2. Wait for: container healthy, ISR back to 3 for every partition, exporter metrics green.
3. Repeat for kafka-2, kafka-3.
4. Schema Registry, Connect, services follow the same one-at-a-time pattern.

## Acceptance
- Producer error count stays 0 (acks=all + retries absorb the elections).
- Consumer lag stays bounded (< threshold of ConsumerLagHigh) and drains after each step.

## Prevent
- Rehearse with `make drill-d1`; T12 evidence lives in tests/upgrade/ once executed.

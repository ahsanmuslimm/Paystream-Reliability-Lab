# Runbook: Broker down (D1)

Symptom: a broker process is gone while traffic continues.

## Detect
- Alert `KafkaBrokerCountBelowExpected` (critical, fires within ~1 min).
- Grafana cluster dashboard: brokers < 3, under-replicated partitions climb.

## Diagnose
- `make ps` - which container stopped? `docker logs kafka-<n> --tail 100`.
- `docker compose exec kafka-1 kafka-metadata-quorum.sh --bootstrap-server localhost:29094 --command-config /etc/kafka/secrets/command.properties describe --status` - is the quorum intact?
- Expect: leader election for affected partitions; acks=all producers keep working as long as min.insync.replicas=2 holds.

## Mitigate
- Single broker loss needs no mitigation; traffic flows with elevated risk (N-1 redundancy).
- If a second broker stops (D2 territory), producers with acks=all block by design - restore a broker first.

## Recover
- Restart the failed broker: `docker compose start kafka-<n>`.
- Watch ISR return to 3 per partition; `KafkaPartitionIsrBelowMinIsr` clears.

## Prevent
- Keep broker heap at the WP0.1-measured baseline; check OOM kills in `docker inspect`.
- D1 drill executed twice with evidence; see docs/drills/d1-broker-loss.md.

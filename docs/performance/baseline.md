# Performance Baseline

> **Status: SCAFFOLD - not yet measured.** The reference workstation has no
> Docker at authoring time (2026-10-07), so every number below is the design
> target from the PRD, not a recorded fact. The tables get filled by
> `tests/performance/staged-load.sh` (T10) and `tests/performance/soak.sh`
> (T14) before the Stage 2 gate (M4); the PRD targets become claims only
> when they appear here with hardware stated.

## Hardware (to be stated with the measured run)

| Field | Value |
|---|---|
| CPU | TBD |
| RAM | 23.6 GB (WP0.1 measurement, 2026-10-06) |
| Disk | TBD (free space at test time) |
| OS | Windows 10 / Docker Desktop VM allocation TBD |

## Staged throughput (T10) — method: kafka-producer-perf-test, 512 B records, acks=all, lz4

| Stage | Design target | Measured p99 (ms) | Measured throughput (msg/s) | Bottleneck observed |
|---|---|---|---|---|
| 1,000 msg/s | p99 < 200 ms | — | — | — |
| 2,500 msg/s | p99 < 200 ms | — | — | — |
| 5,000 msg/s | ceiling >= 5,000 msg/s | — | — | — |

## Soak (T14) — method: steady rate, samples every 5 min

| Metric | Acceptance | Measured drift |
|---|---|---|
| notifier heap | no upward trend after warmup | — |
| broker log volume | bounded by 7-day retention | — |
| consumer lag | returns to ~0 after rate changes | — |
| JVM threads / fds | stable | — |

## Environment note

The WP0.1 reduced-heap variant (`KAFKA_HEAP_OPTS=-Xmx1g -Xms1g`, services
512 MB) is the committed baseline; if the 5,000 msg/s stage misses the
ceiling on this hardware, the reduced-rate variant is recorded here instead
of silently dropping the target (plan R2 mitigation).

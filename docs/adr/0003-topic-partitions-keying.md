# ADR-0003: Transaction topic partition count and keying

Status: accepted (2026-10-06)
Drivers: WP0.2/P1 observations, Document 03 rationale, FR-04

## Context

`bank.transactions.v1` must carry 1–5,000 msg/s on the reference environment
with p99 produce-to-consume latency under 200 ms at 1,000 msg/s (design
targets until measured). The velocity rule needs per-account ordering.

## Decision

**6 partitions**, keyed by `account_id` (UUID string from the shared 200-account pool).

- 6 partitions spread produce load across all three brokers with RF=3 and
  tolerate one broker loss without losing per-account ordering.
- Keying by account_id guarantees per-account ordering, which the velocity
  window (Stage 2) depends on.
- The partition count is a deliberate design decision recorded here: changing
  it later remaps keys to partitions and breaks the velocity window's
  assumptions, so `bank.transactions.v2` would be used instead.

## Consequences

- Per-account ordering holds for all consumer instances.
- Consumer parallelism per instance is capped at 6 for the transactions topic.
- The 200-account synthetic pool concentrates key skew; acceptable for the
  lab because every drill measures cluster behaviour, not key cardinality.

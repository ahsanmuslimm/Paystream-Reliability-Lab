# ADR-0001: Cluster topology - three combined broker+controller nodes (lab)

Status: accepted (2026-10-06)
Drivers: WP0.1 environment baseline, FR-01, G0

## Context

The lab needs a 3-node Kafka cluster in KRaft mode on a 23.6 GB RAM
workstation. KRaft allows brokers to also act as controllers (combined mode)
or to separate roles. The production-style path (Document 02) calls for
dedicated controllers.

## Decision

Run **three combined broker+controller nodes** in Docker Compose for the lab
stages. Each node carries both roles with the quorum voters
`1@kafka-1:29093,2@kafka-2:29093,3@kafka-3:29093`. The dedicated-controller
layout (3 controllers + 3+ brokers) is documented for the production-style
path and will be used if the Stage 3 Kubernetes deployment is built.

## Consequences

- Fits the reference environment: 3 × 1 GB broker heaps leave headroom for
  Schema Registry, PostgreSQL, monitoring and three JVM services.
- Combined mode couples broker failure to controller availability on that
  node - acceptable for drills D1/D2, which is exactly what they demonstrate.
- Compose file and drills stay simple; no ZooKeeper anywhere.

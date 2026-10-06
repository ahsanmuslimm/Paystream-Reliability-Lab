# ADR-0002: Listener and security layout

Status: accepted (2026-10-06, security overlay lands in Stage 2)
Drivers: WP0.5 planning, FR-08

## Context

Kafka needs distinct listeners for inter-broker traffic, client traffic and
(mTLS-secured) controller traffic. Document 02 fixes the security target:
mTLS between brokers, SASL/SCRAM-SHA-512 over TLS for clients, one mTLS-only
demo client.

## Decision

Three listeners per broker from day one:

| Listener | Port | Purpose | Security protocol (final) |
|---|---|---|---|
| `PLAINTEXT` | 29092 | clients inside the Compose network | SASL_SSL (Stage 2) |
| `CONTROLLER` | 29093 | KRaft quorum | mTLS (Stage 2) |
| `PLAINTEXT_HOST` | 1909x | host-side tooling | SASL_SSL (Stage 2) |

The MVP ships the listeners as PLAINTEXT with the map already defined
(`CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT`); the
Stage 2 security overlay (`docker-compose.security.yml`) switches the protocols
to SASL_SSL/mTLS and adds the StandardAuthorizer with default-deny.

## Consequences

- No topology rework is needed when security lands; only protocol maps and
  client configs change, which keeps drill evidence comparable.
- Plaintext remains the MVP known limitation (tracked in README) and is
  acceptable only inside the Compose bridge network on a laptop.

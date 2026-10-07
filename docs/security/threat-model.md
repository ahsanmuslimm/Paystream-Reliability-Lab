# Threat Model - PayStream Reliability Lab

Scope: the single-host Compose deployment (MVP and secured modes). This is a
production-style lab; the threat model is a documentation exercise mapped to
STRIDE, not a certified security assessment (plan section 2.4).

## Assets

1. Transaction/fraud-alert events (synthetic, but the envelope format mirrors real financial events).
2. SCRAM credentials and TLS private keys (`security/secrets/`, `security/certs/`).
3. PostgreSQL account/notification data.
4. The Kafka cluster's integrity itself (configuration, ACLs, topic data).

## STRIDE analysis

| Threat | Vector in this lab | Control | Residual risk |
|---|---|---|---|
| Spoofing | A container impersonating a service principal | SCRAM-SHA-512 auth on client listeners; mTLS client-auth on BROKER/CONTROLLER; no wildcard principals | Lab passwords are generated locally but stored with default file permissions; Stage 3 moves to Vault |
| Tampering | In-flight message modification | TLS on all listeners (FIPS-grade ciphers by JDK default); idempotent producers prevent retry forks | Broker compromise is out of scope on a single host |
| Repudiation | "Who produced this alert?" | `processed_events` + `fraud_alerts` tables record provenance; DLQ headers record failure origin | No external audit log sink in v1 |
| Information disclosure | Plaintext sniffing on the Docker bridge | All listeners SASL_SSL or SSL; PLAINTEXT retained only in MVP mode (documented limitation) | Schema Registry HTTP endpoint is unauthenticated inside the network - lab-only |
| Denial of service | Lag storm, disk fill, poison floods | Consumer-lag and disk alerts; DLQ isolation; velocity rule bounding alert fan-out | No per-principal quotas (KIP-1073 quotas are a Stage 3 candidate) |
| Elevation of privilege | A service using credentials beyond its role | Default-deny StandardAuthorizer; ACL matrix enforced; negative tests in tests/security/ | Superuser list is fixed in the security overlay; human access uses the kafka-setup/svc-admin-ci identities only |

## Trust boundaries

- Host -> Compose network: TLS (PLAINTEXT_HOST listener, SASL_SSL).
- Inside the Compose network: TLS everywhere in secured mode; MVP mode relies on the bridge being host-local (explicitly a known limitation).
- CI (svc-admin-ci) -> cluster: SCRAM over SASL_SSL, cluster-scoped CREATE/ALTER/DESCRIBE only.

## Explicit non-goals

PCI-DSS/SOC 2 certification; container image hardening beyond non-root
multi-stage builds; network egress control between Compose services.

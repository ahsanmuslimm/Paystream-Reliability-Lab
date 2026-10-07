# ACL Matrix - runtime authority

Source of truth: `kafka-config/acls.yaml` (applied by
`kafka-config/scripts/apply-acls.sh`). This document mirrors it for
reviewers; validate-config fails CI when the two can drift in *structure*
(topics/principals named here must exist in the YAML). Document 03 section 6
plus the ADR-0006 additions are marked.

Rules enforced by the matrix and by validate-config:

- no wildcard principals, no `ALL` on `*` resources;
- human users never use service credentials;
- default deny: `allow.everyone.if.no.acl.found=false`;
- every ACL is declared as code.

| Principal | Resource | Operations | Origin |
|---|---|---|---|
| User:svc-txn-producer | topic bank.transactions.v1 | WRITE, DESCRIBE | Doc 03 |
| User:svc-fraud | topic bank.transactions.v1 | READ, DESCRIBE | Doc 03 |
| User:svc-fraud | group fraud-detector | READ | Doc 03 |
| User:svc-fraud | topic bank.fraud-alerts.v1 | WRITE, DESCRIBE | Doc 03 |
| User:svc-fraud | topic bank.transactions.v1.retry / .dlq | READ, WRITE, DESCRIBE | Doc 03 |
| User:svc-fraud | topic prefixed fraud-detector- (Streams internals) | ALL | Doc 03 |
| User:svc-notify | topic bank.fraud-alerts.v1 | READ, DESCRIBE | Doc 03 |
| User:svc-notify | group notifier | READ | Doc 03 |
| User:svc-notify | topic bank.notifications.v1 | WRITE, DESCRIBE | Doc 03 |
| User:svc-notify | topic bank.fraud-alerts.v1.retry / .dlq | READ, WRITE, DESCRIBE | ADR-0006 |
| User:svc-connect | topic prefixed pg. / connect- | READ, WRITE, CREATE, DESCRIBE | Doc 03 |
| User:svc-connect | group connect-cluster | READ | ADR-0006 |
| User:svc-schema-registry | topic _schemas | READ, WRITE, CREATE, DESCRIBE, DESCRIBE_CONFIGS, ALTER_CONFIGS | ADR-0006 |
| User:svc-schema-registry | cluster | DESCRIBE | ADR-0006 |
| User:svc-monitor | cluster | DESCRIBE, DESCRIBE_CONFIGS | ADR-0006 |
| User:svc-monitor | topic prefixed bank. / pg. / connect- | DESCRIBE | ADR-0006 |
| User:svc-monitor | group fraud-detector, notifier | READ | ADR-0006 |
| User:svc-admin-ci | cluster | CREATE, ALTER, DESCRIBE | Doc 03 |
| User:CN=broker-1..3, User:CN=kafka-setup | cluster | super users (cert identity, inter-broker + one-shot bootstrap only) | ADR-0002 |

## Negative test expectations (T8)

For every service principal, each operation NOT in the table must be denied
by the broker. `tests/security/acl-negative-tests.sh` asserts the most
valuable rejections:

- svc-notify cannot WRITE to bank.transactions.v1;
- svc-fraud cannot READ bank.notifications.v1;
- svc-txn-producer cannot consume any topic;
- svc-connect cannot WRITE to bank.* topics;
- no service principal can create topics (CREATE lives with svc-admin-ci).

A "wrong password is refused" and a "plaintext on a secured listener is
refused" check complete T8; see the D5 drill for the expired-certificate
rejection.

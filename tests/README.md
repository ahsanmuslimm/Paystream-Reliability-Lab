# tests/ — cross-service and scripted verification suites

| Directory | Suite | Tools | Stage |
|---|---|---|---|
| `integration/` | Cross-service behaviour: duplicate delivery → one effect; DLQ routing; restart resumes | Testcontainers / Compose | 2 |
| `security/` | `t8-security-tests.sh`: plaintext refused; wrong SCRAM credentials refused; expired cert refused; ACL denials per principal | bash + openssl | 2 |
| `contract/` | Schema evolution: field add with default passes; removal/type change fail | Registry compatibility API | 2 |
| `performance/` | `staged-load.sh` (T10) + `soak.sh` (T14): throughput ceiling, p99, drift sampling | kafka-producer/consumer-perf-test | 2 |
| `upgrade/` | `rolling-restart.sh` (T12) + `docker-compose.expansion.yml` + `reassign-partitions.sh` (T13) | kafka scripts + compose | 2 |

Service-level unit and topology tests live inside the Maven modules
(`services/*/src/test`). The MVP Testcontainers example is
`services/notifier/src/test/java/com/paystream/notifier/NotifierPostgresIT.java`.

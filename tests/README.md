# tests/ — cross-service and scripted verification suites

| Directory | Suite | Tools | Stage |
|---|---|---|---|
| `integration/` | Cross-service behaviour: duplicate delivery → one effect; DLQ routing; restart resumes | Testcontainers / Compose | 2 |
| `security/` | Plaintext refused; wrong SCRAM credentials refused; expired cert refused; ACL denials | scripts + openssl s_client | 2 |
| `contract/` | Schema evolution: field add with default passes; removal/type change fail | Registry compatibility API | 2 |
| `performance/` | Throughput ceiling and p99 at 1k/2.5k/5k msg/s | kafka-producer/consumer-perf-test | 2 |
| `upgrade/` | Rolling restart/upgrade under load with zero producer errors; 4th-broker expansion | kafka scripts | 2 |

Service-level unit and topology tests live inside the Maven modules
(`services/*/src/test`). The MVP Testcontainers example is
`services/notifier/src/test/java/com/paystream/notifier/NotifierPostgresIT.java`.

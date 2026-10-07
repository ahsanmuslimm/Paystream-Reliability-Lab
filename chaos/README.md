# chaos/ — failure-drill programme

One script per drill (D1–D6), shared helpers in `lib.sh`, and timestamped
evidence under `results/` (one folder per run: hypothesis, captured state,
alerts, outcome). Each drill is executed twice with consistent
results before its report is accepted (FR-12, T11) — a drill that cannot be
re-run does not count.

| Script | Drill | Fault injected |
|---|---|---|
| `d1_broker_loss.sh` | Single broker loss | Stop one broker mid-traffic |
| `d2_isr_shrink.sh` | ISR shrink / quorum loss | Stop two brokers |
| `d3_consumer_lag.sh` | Lag + rebalance storm | Notifier member churn under load |
| `d4_disk_full.sh` | Disk exhaustion | Fill broker log volume |
| `d5_cert_expiry.sh` | Certificate expiry | Let a cert expire |
| `d6_poison_message.sh` | Poison message | Publish malformed record |

Delivery: Stage 2, WP2.7 (Weeks 4–5 of the implementation plan). Run via
`make drill-d1` … `make drill-d6`.

# Drill reports D1–D6

One report per drill, written from the evidence captured by the matching
`chaos/d*.sh` run: hypothesis, setup, fault-injection steps, observed
behaviour (metrics/log excerpts), root cause, fix, prevention, and the alert
that fired (FR-12, plan §4.3 WP2.7).

| Report | Script | Status |
|---|---|---|
| `d1-broker-loss.md` | chaos/d1_broker_loss.sh | ⬜ pending execution (needs Docker) |
| `d2-isr-shrink.md` | chaos/d2_isr_shrink.sh | ⬜ pending execution |
| `d3-consumer-lag.md` | chaos/d3_consumer_lag.sh | ⬜ pending execution |
| `d4-disk-full.md` | chaos/d4_disk_full.sh | ⬜ pending execution |
| `d5-cert-expiry.md` | chaos/d5_cert_expiry.sh | ⬜ pending execution |
| `d6-poison-message.md` | chaos/d6_poison_message.sh | ⬜ pending execution |

Acceptance (T11): each drill is executed **twice** with consistent results;
evidence packs live in `chaos/results/<drill>-<timestamp>/`. The reports and
their runbook counterparts (docs/runbooks/) are linked both ways.

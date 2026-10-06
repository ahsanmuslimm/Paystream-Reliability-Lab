-- Document 03 section 4.3
CREATE TABLE fraud_alerts (
  alert_id     UUID PRIMARY KEY,
  txn_id       UUID NOT NULL,
  account_id   UUID NOT NULL REFERENCES accounts(account_id),
  rule_name    VARCHAR(64) NOT NULL,
  severity     VARCHAR(8) NOT NULL CHECK (severity IN ('LOW','MEDIUM','HIGH')),
  details      JSONB,
  detected_at  TIMESTAMPTZ NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_alerts_account_time ON fraud_alerts(account_id, detected_at DESC);

CREATE TABLE notifications (
  notification_id UUID PRIMARY KEY,
  alert_id        UUID NOT NULL UNIQUE REFERENCES fraud_alerts(alert_id),
  customer_id     UUID NOT NULL,
  channel         VARCHAR(8) NOT NULL CHECK (channel IN ('SMS','EMAIL','PUSH')),
  status          VARCHAR(12) NOT NULL DEFAULT 'SENT',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

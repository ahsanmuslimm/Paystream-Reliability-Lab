-- Document 03 section 4.1
CREATE TABLE accounts (
  account_id   UUID PRIMARY KEY,
  customer_id  UUID NOT NULL,
  balance      NUMERIC(14,2) NOT NULL CHECK (balance >= 0),
  status       VARCHAR(16) NOT NULL DEFAULT 'ACTIVE'
               CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_accounts_customer ON accounts(customer_id);

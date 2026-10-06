-- Document 03 section 4.4 (operational tables; used by the drill programme)
CREATE TABLE drill_runs (
  id            BIGSERIAL PRIMARY KEY,
  drill_code    VARCHAR(4) NOT NULL,          -- D1..D6
  started_at    TIMESTAMPTZ NOT NULL,
  ended_at      TIMESTAMPTZ,
  hypothesis    TEXT NOT NULL,
  observed      TEXT,
  root_cause    TEXT,
  fix           TEXT,
  evidence_ref  TEXT,                         -- path/URL to dashboards, logs
  passed        BOOLEAN
);

CREATE TABLE ops_change_log (
  id            BIGSERIAL PRIMARY KEY,
  env           VARCHAR(8) NOT NULL CHECK (env IN ('dev','uat','prod')),
  change_type   VARCHAR(32) NOT NULL,         -- TOPIC, ACL, UPGRADE, CERT_ROTATION
  description   TEXT NOT NULL,
  performed_by  VARCHAR(100) NOT NULL,
  git_commit    VARCHAR(40),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

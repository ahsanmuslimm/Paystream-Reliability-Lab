-- Document 03 section 4.2
CREATE TABLE processed_events (
  consumer_group  VARCHAR(100) NOT NULL,
  event_id        UUID NOT NULL,
  topic           VARCHAR(249) NOT NULL,
  partition_no    INT NOT NULL,
  offset_no       BIGINT NOT NULL,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id)
);

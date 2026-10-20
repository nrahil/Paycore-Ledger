CREATE TABLE webhook_event_work (
    event_id        UUID PRIMARY KEY,
    raw_payload     BYTEA NOT NULL,
    status          VARCHAR(16) NOT NULL CHECK (status IN ('PROCESSING', 'COMPLETE')),
    lease_until     TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_webhook_event_work_expired
    ON webhook_event_work (lease_until, event_id)
    WHERE status = 'PROCESSING';

ALTER TABLE outbox_events
    ADD COLUMN claim_token UUID,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN last_error TEXT;

CREATE INDEX idx_outbox_due_unpublished
    ON outbox_events (next_attempt_at, created_at, id)
    WHERE published_at IS NULL;

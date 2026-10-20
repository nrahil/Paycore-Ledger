ALTER TABLE webhook_deliveries
    DROP CONSTRAINT webhook_deliveries_status_check;

ALTER TABLE webhook_deliveries
    ADD CONSTRAINT webhook_deliveries_status_check
        CHECK (status IN ('DELIVERED', 'RETRY_SCHEDULED', 'DEAD_LETTERED', 'FAILED', 'DELIVERING')),
    ADD COLUMN claim_token UUID,
    ADD COLUMN lease_until TIMESTAMPTZ;

CREATE INDEX idx_webhook_deliveries_expired_claims
    ON webhook_deliveries (lease_until, id)
    WHERE status = 'DELIVERING';

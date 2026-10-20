-- =============================================================================
-- V4: Durable phase-1 claims for capture/refund PSP operations
-- =============================================================================

ALTER TABLE payments DROP CONSTRAINT IF EXISTS payments_status_check;

ALTER TABLE payments ADD CONSTRAINT payments_status_check CHECK (status IN (
    'CREATED',
    'AUTHORIZING',
    'AUTHORIZED',
    'CAPTURING',
    'CAPTURED',
    'PARTIALLY_REFUNDED',
    'REFUNDED',
    'VOIDED',
    'FAILED'
));

CREATE TABLE payment_operations (
    id              UUID PRIMARY KEY,
    merchant_id     UUID NOT NULL REFERENCES merchants(id),
    payment_id      UUID NOT NULL REFERENCES payments(id) ON DELETE CASCADE,
    operation_type  VARCHAR(16) NOT NULL CHECK (operation_type IN ('CAPTURE', 'REFUND')),
    amount          BIGINT NOT NULL CHECK (amount > 0),
    fee_amount      BIGINT NOT NULL DEFAULT 0 CHECK (fee_amount >= 0),
    status          VARCHAR(16) NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payment_operations_in_progress
    ON payment_operations (payment_id, operation_type, created_at DESC)
    WHERE status = 'IN_PROGRESS';

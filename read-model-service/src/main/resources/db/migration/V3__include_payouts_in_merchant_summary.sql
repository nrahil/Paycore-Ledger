ALTER TABLE merchant_daily_summary
    ADD COLUMN payout_amount BIGINT NOT NULL DEFAULT 0;

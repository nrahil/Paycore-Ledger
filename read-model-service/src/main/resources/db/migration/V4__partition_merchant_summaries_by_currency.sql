ALTER TABLE merchant_daily_summary
    DROP CONSTRAINT merchant_daily_summary_pkey;

ALTER TABLE merchant_daily_summary
    ADD COLUMN currency VARCHAR(3) NOT NULL DEFAULT 'INR';

ALTER TABLE merchant_daily_summary
    ADD PRIMARY KEY (merchant_id, day, currency);

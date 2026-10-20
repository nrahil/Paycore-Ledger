ALTER TABLE recon_discrepancies
    DROP CONSTRAINT recon_discrepancies_kind_check;

ALTER TABLE recon_discrepancies
    ADD CONSTRAINT recon_discrepancies_kind_check CHECK (kind IN (
        'MISSING_IN_LEDGER',
        'MISSING_IN_PSP_FILE',
        'AMOUNT_MISMATCH',
        'STATUS_MISMATCH',
        'CURRENCY_MISMATCH'
    ));

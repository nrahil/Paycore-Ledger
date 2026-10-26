package com.paycore.reconciliation;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** API and persistence-neutral records shared by the reconciliation use cases. */
public final class ReconciliationModels {
    private ReconciliationModels() {}

    public enum DiscrepancyKind {
        MISSING_IN_LEDGER, MISSING_IN_PSP_FILE, AMOUNT_MISMATCH, STATUS_MISMATCH, CURRENCY_MISMATCH
    }

    public record InternalPaymentRecord(
            UUID paymentId, String pspRef, String status, long expectedSettlementAmount,
            String currency, boolean hasCaptureLedgerTxn) {}

    public record DiscrepancyRecord(
            UUID id, UUID runId, String pspRef, UUID paymentId, DiscrepancyKind kind,
            Long expectedAmt, Long actualAmt, Map<String, Object> details) {}

    public record ReconRunReport(
            UUID runId, LocalDate settlementDate, int matched, int discrepancyCount,
            Instant createdAt, List<DiscrepancyRecord> discrepancies) {}
}

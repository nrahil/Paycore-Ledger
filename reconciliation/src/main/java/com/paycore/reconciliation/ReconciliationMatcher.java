package com.paycore.reconciliation;

import static com.paycore.reconciliation.ReconciliationModels.DiscrepancyKind;
import static com.paycore.reconciliation.ReconciliationModels.DiscrepancyRecord;
import static com.paycore.reconciliation.ReconciliationModels.InternalPaymentRecord;

import com.paycore.common.ids.UuidV7;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.stereotype.Component;

/** Applies settlement matching policy to already parsed PSP rows and ledger-backed payment facts. */
@Component
public class ReconciliationMatcher {

    public MatchResult match(
            UUID runId,
            Map<String, PspSettlementRow> pspRowsByReference,
            Map<String, InternalPaymentRecord> internalByReference,
            List<InternalPaymentRecord> capturedPaymentsForDate) {
        List<DiscrepancyRecord> discrepancies = new ArrayList<>();
        int matched = 0;

        for (PspSettlementRow pspRow : pspRowsByReference.values()) {
            InternalPaymentRecord internal = internalByReference.get(pspRow.pspRef());
            if (internal == null || ("SETTLED".equalsIgnoreCase(pspRow.status()) && !internal.hasCaptureLedgerTxn())) {
                discrepancies.add(discrepancy(runId, pspRow.pspRef(), internal == null ? null : internal.paymentId(),
                        DiscrepancyKind.MISSING_IN_LEDGER, null, pspRow.amount(),
                        Map.of("pspStatus", pspRow.status(), "settledAt", pspRow.settledAt(),
                                "reason", internal == null ? "psp_ref not found in payments table"
                                        : "payment has no CAPTURE ledger transaction")));
                continue;
            }
            if (!isStatusCompatible(pspRow.status(), internal.status())) {
                discrepancies.add(discrepancy(runId, pspRow.pspRef(), internal.paymentId(),
                        DiscrepancyKind.STATUS_MISMATCH, internal.expectedSettlementAmount(), pspRow.amount(),
                        Map.of("expectedStatus", internal.status(), "actualPspStatus", pspRow.status(),
                                "settledAt", pspRow.settledAt())));
                continue;
            }
            if (!pspRow.currency().equals(internal.currency())) {
                discrepancies.add(discrepancy(runId, pspRow.pspRef(), internal.paymentId(),
                        DiscrepancyKind.CURRENCY_MISMATCH, null, null,
                        Map.of("expectedCurrency", internal.currency(), "actualCurrency", pspRow.currency())));
                continue;
            }
            long expectedAmount = internal.expectedSettlementAmount();
            if (expectedAmount != pspRow.amount()) {
                discrepancies.add(discrepancy(runId, pspRow.pspRef(), internal.paymentId(),
                        DiscrepancyKind.AMOUNT_MISMATCH, expectedAmount, pspRow.amount(),
                        Map.of("status", internal.status(), "pspStatus", pspRow.status(),
                                "deltaPaise", pspRow.amount() - expectedAmount)));
                continue;
            }
            matched++;
        }

        for (InternalPaymentRecord internal : capturedPaymentsForDate) {
            if (isCapturedStatus(internal.status()) && !pspRowsByReference.containsKey(internal.pspRef())) {
                discrepancies.add(discrepancy(runId, internal.pspRef(), internal.paymentId(),
                        DiscrepancyKind.MISSING_IN_PSP_FILE, internal.expectedSettlementAmount(), null,
                        Map.of("internalStatus", internal.status(),
                                "reason", "CAPTURED payment in ledger missing from PSP settlement CSV")));
            }
        }
        return new MatchResult(matched, List.copyOf(discrepancies));
    }

    private static DiscrepancyRecord discrepancy(UUID runId, String pspRef, UUID paymentId,
            DiscrepancyKind kind, Long expected, Long actual, Map<String, Object> details) {
        return new DiscrepancyRecord(UuidV7.generate(), runId, pspRef, paymentId, kind, expected, actual, details);
    }

    private static boolean isCapturedStatus(String status) {
        return "CAPTURED".equals(status) || "PARTIALLY_REFUNDED".equals(status) || "REFUNDED".equals(status);
    }

    private static boolean isStatusCompatible(String pspStatus, String internalStatus) {
        String psp = pspStatus.trim().toUpperCase();
        String internal = internalStatus.trim().toUpperCase();
        return switch (psp) {
            case "SETTLED", "CAPTURED" -> "CAPTURED".equals(internal)
                    || "PARTIALLY_REFUNDED".equals(internal) || "REFUNDED".equals(internal);
            case "AUTHORIZED" -> "AUTHORIZED".equals(internal);
            case "VOIDED" -> "VOIDED".equals(internal);
            case "FAILED" -> "FAILED".equals(internal);
            default -> psp.equals(internal);
        };
    }

    public record MatchResult(int matched, List<DiscrepancyRecord> discrepancies) {}
}

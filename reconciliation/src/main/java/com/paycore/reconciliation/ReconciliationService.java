package com.paycore.reconciliation;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.ids.UuidV7;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import static com.paycore.reconciliation.ReconciliationModels.DiscrepancyKind;
import static com.paycore.reconciliation.ReconciliationModels.DiscrepancyRecord;
import static com.paycore.reconciliation.ReconciliationModels.InternalPaymentRecord;
import static com.paycore.reconciliation.ReconciliationModels.ReconRunReport;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Settlement Reconciliation Engine (FR12, Section 5.1, Section 7.10).
 *
 * <p>Ingests a PSP daily settlement CSV ({@code psp_ref,amount,status,settled_at}) and compares
 * every row against PayCore's {@code payments} and {@code ledger_transactions} tables,
 * flagging all 4 discrepancy kinds:
 * <ul>
 *   <li>{@code MISSING_IN_LEDGER}</li>
 *   <li>{@code MISSING_IN_PSP_FILE}</li>
 *   <li>{@code AMOUNT_MISMATCH}</li>
 *   <li>{@code STATUS_MISMATCH}</li>
 * </ul>
 */
@Service
public class ReconciliationService {

    private final ClockProvider clockProvider;
    private final SettlementCsvParser settlementCsvParser;
    private final ReconciliationMatcher reconciliationMatcher;
    private final ReconciliationRepository reconciliationRepository;

    public ReconciliationService(
            ClockProvider clockProvider,
            SettlementCsvParser settlementCsvParser,
            ReconciliationMatcher reconciliationMatcher,
            ReconciliationRepository reconciliationRepository) {
        this.clockProvider = clockProvider;
        this.settlementCsvParser = settlementCsvParser;
        this.reconciliationMatcher = reconciliationMatcher;
        this.reconciliationRepository = reconciliationRepository;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ReconRunReport runReconciliation(LocalDate settlementDate, String csvContent) {
        Map<String, PspSettlementRow> csvRowsByPspRef = settlementCsvParser.parse(csvContent, settlementDate);
        List<InternalPaymentRecord> internalRecords = reconciliationRepository.findPaymentsWithLedgerMovement(
                settlementDate, csvRowsByPspRef.keySet());
        Map<String, InternalPaymentRecord> internalByPspRef = indexByPspReference(internalRecords);

        UUID runId = UuidV7.generate();
        Instant now = clockProvider.now();
        ReconciliationMatcher.MatchResult result = reconciliationMatcher.match(
                runId, csvRowsByPspRef, internalByPspRef, internalRecords);
        List<DiscrepancyRecord> discrepancies = result.discrepancies();
        int matched = result.matched();

        reconciliationRepository.saveRun(runId, settlementDate, matched, discrepancies, now);

        return new ReconRunReport(
                runId,
                settlementDate,
                matched,
                discrepancies.size(),
                now,
                discrepancies);
    }

    @Transactional(readOnly = true)
    public List<DiscrepancyRecord> getDiscrepanciesForRun(UUID runId) {
        return reconciliationRepository.findDiscrepancies(runId);
    }

    private Map<String, InternalPaymentRecord> indexByPspReference(List<InternalPaymentRecord> records) {
        Map<String, InternalPaymentRecord> byPspRef = new java.util.LinkedHashMap<>();
        for (InternalPaymentRecord record : records) {
            byPspRef.put(record.pspRef(), record);
        }
        return byPspRef;
    }

}

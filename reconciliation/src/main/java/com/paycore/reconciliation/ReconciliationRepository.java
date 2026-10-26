package com.paycore.reconciliation;

import static com.paycore.reconciliation.ReconciliationModels.DiscrepancyKind;
import static com.paycore.reconciliation.ReconciliationModels.DiscrepancyRecord;
import static com.paycore.reconciliation.ReconciliationModels.InternalPaymentRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Owns reconciliation SQL and row mapping; transaction scope remains with the use-case service. */
@Repository
public class ReconciliationRepository {
    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;

    public ReconciliationRepository(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
    }

    public List<InternalPaymentRecord> findPaymentsWithLedgerMovement(LocalDate date, Set<String> csvRefs) {
        var startOfDay = date.atStartOfDay(ZoneOffset.UTC).toInstant();
        var endOfDay = date.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant();
        return jdbcClient.sql("""
                SELECT p.id AS payment_id, p.psp_ref, p.status, p.currency,
                    COALESCE(SUM(CASE
                        WHEN a.name LIKE 'PSP_RECEIVABLE%' AND a.currency = p.currency AND le.direction = 'DEBIT' THEN le.amount
                        WHEN a.name LIKE 'PSP_RECEIVABLE%' AND a.currency = p.currency AND le.direction = 'CREDIT' THEN -le.amount
                        ELSE 0 END), 0) AS ledger_settlement_amount,
                    EXISTS (SELECT 1 FROM ledger_transactions capture
                            WHERE capture.payment_id = p.id AND capture.kind = 'CAPTURE') AS has_capture_txn
                FROM payments p
                LEFT JOIN ledger_transactions movement ON movement.payment_id = p.id
                    AND movement.created_at >= :startOfDay AND movement.created_at < :endOfDay
                LEFT JOIN ledger_entries le ON le.txn_id = movement.id
                LEFT JOIN accounts a ON a.id = le.account_id
                WHERE p.psp_ref IS NOT NULL
                  AND (p.psp_ref IN (:csvRefs) OR EXISTS (
                       SELECT 1 FROM ledger_transactions activity
                       WHERE activity.payment_id = p.id AND activity.kind IN ('CAPTURE', 'REFUND')
                         AND activity.created_at >= :startOfDay AND activity.created_at < :endOfDay))
                GROUP BY p.id, p.psp_ref, p.status, p.currency
                """)
                .param("startOfDay", Timestamp.from(startOfDay))
                .param("endOfDay", Timestamp.from(endOfDay))
                .param("csvRefs", csvRefs.isEmpty() ? List.of("__no_psp_refs__") : csvRefs)
                .query((rs, rowNum) -> new InternalPaymentRecord(
                        rs.getObject("payment_id", UUID.class), rs.getString("psp_ref"), rs.getString("status"),
                        rs.getLong("ledger_settlement_amount"), rs.getString("currency"), rs.getBoolean("has_capture_txn")))
                .list();
    }

    public void saveRun(UUID runId, LocalDate settlementDate, int matched, List<DiscrepancyRecord> discrepancies,
            java.time.Instant createdAt) {
        jdbcClient.sql("""
                INSERT INTO recon_runs (id, settlement_date, matched, discrepancies, created_at)
                VALUES (:id, :settlementDate, :matched, :discrepancies, :createdAt)
                """)
                .param("id", runId).param("settlementDate", java.sql.Date.valueOf(settlementDate))
                .param("matched", matched).param("discrepancies", discrepancies.size())
                .param("createdAt", Timestamp.from(createdAt)).update();
        for (DiscrepancyRecord d : discrepancies) {
            jdbcClient.sql("""
                    INSERT INTO recon_discrepancies (id, run_id, psp_ref, payment_id, kind, expected_amt, actual_amt, details)
                    VALUES (:id, :runId, :pspRef, :paymentId, :kind, :expectedAmt, :actualAmt, CAST(:details AS jsonb))
                    """)
                    .param("id", d.id()).param("runId", d.runId()).param("pspRef", d.pspRef())
                    .param("paymentId", d.paymentId()).param("kind", d.kind().name())
                    .param("expectedAmt", d.expectedAmt()).param("actualAmt", d.actualAmt())
                    .param("details", writeDetails(d.details())).update();
        }
    }

    public List<DiscrepancyRecord> findDiscrepancies(UUID runId) {
        return jdbcClient.sql("""
                SELECT id, run_id, psp_ref, payment_id, kind, expected_amt, actual_amt, details::text AS details_text
                FROM recon_discrepancies WHERE run_id = :runId ORDER BY id ASC
                """)
                .param("runId", runId)
                .query((rs, rowNum) -> {
                    Long expected = rs.getLong("expected_amt");
                    if (rs.wasNull()) expected = null;
                    Long actual = rs.getLong("actual_amt");
                    if (rs.wasNull()) actual = null;
                    return new DiscrepancyRecord(rs.getObject("id", UUID.class), rs.getObject("run_id", UUID.class),
                            rs.getString("psp_ref"), rs.getObject("payment_id", UUID.class),
                            DiscrepancyKind.valueOf(rs.getString("kind")), expected, actual, readDetails(rs.getString("details_text")));
                }).list();
    }

    private String writeDetails(Map<String, Object> details) {
        try { return objectMapper.writeValueAsString(details); }
        catch (JsonProcessingException failure) { throw new IllegalStateException("Could not serialize reconciliation details", failure); }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> readDetails(String json) {
        try { return objectMapper.readValue(json, Map.class); }
        catch (JsonProcessingException failure) { throw new IllegalStateException("Could not deserialize stored reconciliation details", failure); }
    }
}

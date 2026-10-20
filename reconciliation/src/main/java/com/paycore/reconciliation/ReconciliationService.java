package com.paycore.reconciliation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.ids.UuidV7;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.simple.JdbcClient;
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

    public enum DiscrepancyKind {
        MISSING_IN_LEDGER,
        MISSING_IN_PSP_FILE,
        AMOUNT_MISMATCH,
        STATUS_MISMATCH,
        CURRENCY_MISMATCH
    }

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private final ObjectMapper objectMapper;

    public ReconciliationService(
            JdbcClient jdbcClient, ClockProvider clockProvider, ObjectMapper objectMapper) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.objectMapper = objectMapper;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ReconRunReport runReconciliation(LocalDate settlementDate, String csvContent) {
        Map<String, PspSettlementRow> csvRowsByPspRef = parseSettlementCsv(csvContent, settlementDate);
        Map<String, InternalPaymentRecord> internalByPspRef = loadInternalPayments(settlementDate, csvRowsByPspRef.keySet());

        UUID runId = UuidV7.generate();
        Instant now = clockProvider.now();
        List<DiscrepancyRecord> discrepancies = new ArrayList<>();
        int matched = 0;

        // 1. Check every row in the PSP settlement CSV against internal payments + ledger
        for (PspSettlementRow pspRow : csvRowsByPspRef.values()) {
            InternalPaymentRecord internal = internalByPspRef.get(pspRow.pspRef());
            if (internal == null || ("SETTLED".equalsIgnoreCase(pspRow.status()) && !internal.hasCaptureLedgerTxn())) {
                // Present in PSP settlement file, but missing in payments or missing CAPTURE ledger transaction
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        pspRow.pspRef(),
                        internal != null ? internal.paymentId() : null,
                        DiscrepancyKind.MISSING_IN_LEDGER,
                        null,
                        pspRow.amount(),
                        Map.of(
                                "pspStatus", pspRow.status(),
                                "settledAt", pspRow.settledAt(),
                                "reason", internal == null
                                        ? "psp_ref not found in payments table"
                                        : "payment has no CAPTURE ledger transaction")));
                continue;
            }

            // Verify status compatibility first
            if (!isStatusCompatible(pspRow.status(), internal.status())) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        pspRow.pspRef(),
                        internal.paymentId(),
                        DiscrepancyKind.STATUS_MISMATCH,
                        internal.expectedSettlementAmount(),
                        pspRow.amount(),
                        Map.of(
                                "expectedStatus", internal.status(),
                                "actualPspStatus", pspRow.status(),
                                "settledAt", pspRow.settledAt())));
                continue;
            }

            if (!pspRow.currency().equals(internal.currency())) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(), runId, pspRow.pspRef(), internal.paymentId(),
                        DiscrepancyKind.CURRENCY_MISMATCH, null, null,
                        Map.of("expectedCurrency", internal.currency(), "actualCurrency", pspRow.currency())));
                continue;
            }

            // Verify amount
            long expectedAmt = internal.expectedSettlementAmount();
            if (expectedAmt != pspRow.amount()) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        pspRow.pspRef(),
                        internal.paymentId(),
                        DiscrepancyKind.AMOUNT_MISMATCH,
                        expectedAmt,
                        pspRow.amount(),
                        Map.of(
                                "status", internal.status(),
                                "pspStatus", pspRow.status(),
                                "deltaPaise", pspRow.amount() - expectedAmt)));
                continue;
            }

            matched++;
        }

        // 2. Check every internal CAPTURED / PARTIALLY_REFUNDED / REFUNDED payment (with a psp_ref) to see if it is missing from the PSP CSV
        for (InternalPaymentRecord internal : loadCapturedPaymentsForDate(settlementDate)) {
            if (("CAPTURED".equals(internal.status())
                            || "PARTIALLY_REFUNDED".equals(internal.status())
                            || "REFUNDED".equals(internal.status()))
                    && !csvRowsByPspRef.containsKey(internal.pspRef())) {
                discrepancies.add(new DiscrepancyRecord(
                        UuidV7.generate(),
                        runId,
                        internal.pspRef(),
                        internal.paymentId(),
                        DiscrepancyKind.MISSING_IN_PSP_FILE,
                        internal.expectedSettlementAmount(),
                        null,
                        Map.of(
                                "internalStatus", internal.status(),
                                "reason", "CAPTURED payment in ledger missing from PSP settlement CSV")));
            }
        }

        // Persist recon_runs header
        jdbcClient.sql("""
                INSERT INTO recon_runs (id, settlement_date, matched, discrepancies, created_at)
                VALUES (:id, :settlementDate, :matched, :discrepancies, :createdAt)
                """)
                .param("id", runId)
                .param("settlementDate", Date.valueOf(settlementDate))
                .param("matched", matched)
                .param("discrepancies", discrepancies.size())
                .param("createdAt", Timestamp.from(now))
                .update();

        // Persist recon_discrepancies rows
        for (DiscrepancyRecord d : discrepancies) {
            String detailsJson;
            try {
                detailsJson = objectMapper.writeValueAsString(d.details());
            } catch (Exception e) {
                detailsJson = "{}";
            }
            jdbcClient.sql("""
                    INSERT INTO recon_discrepancies (
                        id, run_id, psp_ref, payment_id, kind, expected_amt, actual_amt, details
                    ) VALUES (
                        :id, :runId, :pspRef, :paymentId, :kind, :expectedAmt, :actualAmt, CAST(:details AS jsonb)
                    )
                    """)
                    .param("id", d.id())
                    .param("runId", d.runId())
                    .param("pspRef", d.pspRef())
                    .param("paymentId", d.paymentId())
                    .param("kind", d.kind().name())
                    .param("expectedAmt", d.expectedAmt())
                    .param("actualAmt", d.actualAmt())
                    .param("details", detailsJson)
                    .update();
        }

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
        return jdbcClient.sql("""
                SELECT id, run_id, psp_ref, payment_id, kind, expected_amt, actual_amt, details::text AS details_text
                FROM recon_discrepancies
                WHERE run_id = :runId
                ORDER BY id ASC
                """)
                .param("runId", runId)
                .query((rs, rowNum) -> {
                    Long expected = rs.getLong("expected_amt");
                    if (rs.wasNull()) expected = null;
                    Long actual = rs.getLong("actual_amt");
                    if (rs.wasNull()) actual = null;
                    Map<String, Object> detailsMap = Map.of();
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> parsed = objectMapper.readValue(rs.getString("details_text"), Map.class);
                        detailsMap = parsed;
                    } catch (Exception ignored) {
                    }
                    return new DiscrepancyRecord(
                            rs.getObject("id", UUID.class),
                            rs.getObject("run_id", UUID.class),
                            rs.getString("psp_ref"),
                            rs.getObject("payment_id", UUID.class),
                            DiscrepancyKind.valueOf(rs.getString("kind")),
                            expected,
                            actual,
                            detailsMap);
                })
                .list();
    }

    private boolean isStatusCompatible(String pspStatus, String internalStatus) {
        String psp = pspStatus.trim().toUpperCase();
        String internal = internalStatus.trim().toUpperCase();
        return switch (psp) {
            case "SETTLED", "CAPTURED" -> "CAPTURED".equals(internal)
                    || "PARTIALLY_REFUNDED".equals(internal)
                    || "REFUNDED".equals(internal);
            case "AUTHORIZED" -> "AUTHORIZED".equals(internal);
            case "VOIDED" -> "VOIDED".equals(internal);
            case "FAILED" -> "FAILED".equals(internal);
            default -> psp.equals(internal);
        };
    }

    private Map<String, PspSettlementRow> parseSettlementCsv(String csvContent, LocalDate settlementDate) {
        Map<String, PspSettlementRow> map = new LinkedHashMap<>();
        if (csvContent == null || csvContent.isBlank()) {
            return map;
        }
        try (CSVParser parser = CSVParser.parse(csvContent, CSVFormat.DEFAULT)) {
            boolean firstLine = true;
            for (CSVRecord record : parser) {
                if (record.size() == 0) continue;
                if (firstLine && record.get(0).trim().equalsIgnoreCase("psp_ref")) {
                    firstLine = false;
                    continue;
                }
                firstLine = false;
                if (record.size() != 4 && record.size() != 5) {
                    throw new IllegalArgumentException("Invalid CSV record " + record.getRecordNumber()
                            + "; expected 4 columns and optional currency");
                }
                String pspRef = record.get(0).trim();
                long amount = Long.parseLong(record.get(1).trim());
                String status = record.get(2).trim();
                String settledAt = record.get(3).trim();
                String currency = record.size() == 5 ? record.get(4).trim().toUpperCase() : "INR";
                if (pspRef.isEmpty() || status.isEmpty() || settledAt.isEmpty() || currency.isEmpty()) {
                    throw new IllegalArgumentException("Settlement record " + record.getRecordNumber()
                            + " has an empty required field");
                }
                LocalDate rowDate = settlementDate(settledAt);
                if (!rowDate.equals(settlementDate)) {
                    throw new IllegalArgumentException("Settlement row date " + rowDate + " does not match requested date " + settlementDate);
                }
                if (map.putIfAbsent(pspRef, new PspSettlementRow(pspRef, amount, status, settledAt, currency)) != null) {
                    throw new IllegalArgumentException("Duplicate PSP reference in settlement file: " + pspRef);
                }
            }
        } catch (IOException | RuntimeException e) {
            throw new IllegalArgumentException("Failed to parse settlement CSV: " + e.getMessage(), e);
        }
        return map;
    }

    private LocalDate settlementDate(String value) {
        try {
            return LocalDate.parse(value);
        } catch (java.time.format.DateTimeParseException ignored) {
            return Instant.parse(value).atZone(ZoneOffset.UTC).toLocalDate();
        }
    }

    private Map<String, InternalPaymentRecord> loadInternalPayments(LocalDate settlementDate, java.util.Set<String> csvRefs) {
        Instant startOfDay = settlementDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
        Instant endOfDay = settlementDate.plusDays(1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant();
        List<InternalPaymentRecord> records = jdbcClient.sql("""
                SELECT
                    p.id AS payment_id,
                    p.psp_ref,
                    p.status,
                    p.amount,
                    p.currency,
                    COALESCE(SUM(CASE WHEN a.name LIKE 'PSP_RECEIVABLE%' AND a.currency = p.currency AND le.direction = 'DEBIT'
                                      THEN le.amount WHEN a.name LIKE 'PSP_RECEIVABLE%' AND a.currency = p.currency AND le.direction = 'CREDIT'
                                      THEN -le.amount ELSE 0 END), 0) AS ledger_settlement_amount,
                    EXISTS (
                        SELECT 1 FROM ledger_transactions lt
                        WHERE lt.payment_id = p.id AND lt.kind = 'CAPTURE'
                    ) AS has_capture_txn
                FROM payments p
                LEFT JOIN ledger_transactions lt ON lt.payment_id = p.id
                    AND lt.created_at >= :startOfDay AND lt.created_at < :endOfDay
                LEFT JOIN ledger_entries le ON le.txn_id = lt.id
                LEFT JOIN accounts a ON a.id = le.account_id
                WHERE p.psp_ref IS NOT NULL
                  AND (p.psp_ref IN (:csvRefs) OR EXISTS (
                       SELECT 1 FROM ledger_transactions activity
                       WHERE activity.payment_id = p.id AND activity.kind IN ('CAPTURE', 'REFUND')
                         AND activity.created_at >= :startOfDay AND activity.created_at < :endOfDay))
                GROUP BY p.id, p.psp_ref, p.status, p.amount, p.currency
                """)
                .param("startOfDay", Timestamp.from(startOfDay))
                .param("endOfDay", Timestamp.from(endOfDay))
                .param("csvRefs", csvRefs.isEmpty() ? List.of("__no_psp_refs__") : csvRefs)
                .query((rs, rowNum) -> new InternalPaymentRecord(
                        rs.getObject("payment_id", UUID.class),
                        rs.getString("psp_ref"),
                        rs.getString("status"),
                        rs.getLong("ledger_settlement_amount"),
                        rs.getString("currency"),
                        rs.getBoolean("has_capture_txn")))
                .list();

        Map<String, InternalPaymentRecord> byPspRef = new LinkedHashMap<>();
        for (InternalPaymentRecord r : records) {
            byPspRef.put(r.pspRef(), r);
        }
        return byPspRef;
    }

    private List<InternalPaymentRecord> loadCapturedPaymentsForDate(LocalDate date) {
        return new ArrayList<>(loadInternalPayments(date, java.util.Set.of()).values());
    }

    public record PspSettlementRow(String pspRef, long amount, String status, String settledAt, String currency) {}

    public record InternalPaymentRecord(
            UUID paymentId,
            String pspRef,
            String status,
            long expectedSettlementAmount,
            String currency,
            boolean hasCaptureLedgerTxn) {
    }

    public record DiscrepancyRecord(
            UUID id,
            UUID runId,
            String pspRef,
            UUID paymentId,
            DiscrepancyKind kind,
            Long expectedAmt,
            Long actualAmt,
            Map<String, Object> details) {}

    public record ReconRunReport(
            UUID runId,
            LocalDate settlementDate,
            int matched,
            int discrepancyCount,
            Instant createdAt,
            List<DiscrepancyRecord> discrepancies) {}
}

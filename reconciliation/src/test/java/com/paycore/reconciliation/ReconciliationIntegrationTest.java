package com.paycore.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.paycore.common.ids.UuidV7;
import com.paycore.reconciliation.ReconciliationController.ReconciliationRequest;
import com.paycore.reconciliation.ReconciliationService.DiscrepancyKind;
import com.paycore.reconciliation.ReconciliationService.DiscrepancyRecord;
import com.paycore.reconciliation.ReconciliationService.ReconRunReport;
import java.time.LocalDate;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ReconciliationIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_recon_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.security.admin-token", () -> "reconciliation-test-admin");
    }

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private ReconciliationService reconciliationService;

    @BeforeEach
    void setUp() {
        jdbcClient.sql("""
                CREATE TABLE IF NOT EXISTS payments (
                    id UUID PRIMARY KEY,
                    psp_ref TEXT,
                    status TEXT NOT NULL,
                    amount BIGINT NOT NULL,
                    currency TEXT NOT NULL DEFAULT 'INR',
                    captured_amount BIGINT NOT NULL DEFAULT 0,
                    refunded_amount BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT '2026-10-11T10:00:00Z'
                );
                CREATE TABLE IF NOT EXISTS ledger_transactions (
                    id UUID PRIMARY KEY,
                    payment_id UUID NOT NULL,
                    kind TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT '2026-10-11T10:00:00Z'
                );
                CREATE TABLE IF NOT EXISTS accounts (
                    id UUID PRIMARY KEY,
                    type TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS ledger_entries (
                    id UUID PRIMARY KEY,
                    txn_id UUID NOT NULL,
                    account_id UUID NOT NULL,
                    direction TEXT NOT NULL,
                    amount BIGINT NOT NULL
                );
                """).update();
        jdbcClient.sql("TRUNCATE TABLE recon_discrepancies, recon_runs, ledger_entries, ledger_transactions, payments, accounts").update();
        jdbcClient.sql("INSERT INTO accounts (id, type, currency, name) VALUES (:id, 'ASSET', 'INR', 'PSP_RECEIVABLE_00')")
                .param("id", UUID.fromString("00000000-0000-7000-8000-000000000001")).update();
        jdbcClient.sql("INSERT INTO accounts (id, type, currency, name) VALUES (:id, 'LIABILITY', 'INR', 'MERCHANT_PAYABLE')")
                .param("id", UUID.fromString("00000000-0000-7000-8000-000000000002")).update();
    }

    @Test
    @DisplayName("Settlement CSV reconciliation detects all 4 discrepancy kinds and matches PARTIALLY_REFUNDED net amounts")
    void settlementReconciliationDetectsAllFourDiscrepancyKinds() {
        // 1. Insert 3 clean matching payments (including a PARTIALLY_REFUNDED payment with net 20,000 = 25,000 - 5,000)
        insertPaymentWithLedger("psp_match_1", "CAPTURED", 10_000L, 10_000L, 0L, true);
        insertPaymentWithLedger("psp_match_2", "PARTIALLY_REFUNDED", 25_000L, 25_000L, 5_000L, true);
        insertPaymentWithLedger("psp_match_3", "CAPTURED", 50_000L, 50_000L, 0L, true);

        // 2. Insert a CAPTURED payment in PayCore that is MISSING from the PSP CSV -> MISSING_IN_PSP_FILE
        UUID missingInPspId = insertPaymentWithLedger("psp_missing_in_csv", "CAPTURED", 12_000L, 12_000L, 0L, true);

        // 3. Insert a CAPTURED payment in PayCore where PSP CSV reports a different amount -> AMOUNT_MISMATCH
        UUID amountMismatchId = insertPaymentWithLedger("psp_amount_diff", "CAPTURED", 20_000L, 20_000L, 0L, true);

        // 4. Insert an AUTHORIZED (not captured) payment where PSP CSV reports SETTLED -> STATUS_MISMATCH
        UUID statusMismatchId = insertPaymentWithLedger("psp_status_diff", "FAILED", 15_000L, 0L, 0L, false);

        String csv = """
                psp_ref,amount,status,settled_at,currency
                psp_match_1,10000,SETTLED,2026-10-11T10:00:00Z,INR
                psp_match_2,20000,SETTLED,2026-10-11T10:05:00Z,INR
                psp_match_3,50000,SETTLED,2026-10-11T10:10:00Z,INR
                psp_ghost_in_csv,8800,SETTLED,2026-10-11T10:15:00Z,INR
                psp_amount_diff,19500,SETTLED,2026-10-11T10:20:00Z,INR
                psp_status_diff,15000,AUTHORIZED,2026-10-11T10:25:00Z,INR
                """;

        ReconciliationRequest req = new ReconciliationRequest(LocalDate.of(2026, 10, 11), csv);
        org.springframework.http.HttpHeaders adminHeaders = new org.springframework.http.HttpHeaders();
        adminHeaders.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
        adminHeaders.set("X-PayCore-Admin-Token", "reconciliation-test-admin");
        ResponseEntity<ReconRunReport> resp = restTemplate.exchange(
                "/v1/admin/reconciliation", org.springframework.http.HttpMethod.POST,
                new org.springframework.http.HttpEntity<>(req, adminHeaders), ReconRunReport.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        ReconRunReport report = resp.getBody();
        assertThat(report).isNotNull();
        assertThat(report.matched()).isEqualTo(3);
        assertThat(report.discrepancyCount()).isEqualTo(4);
        assertThat(report.discrepancies()).hasSize(4);

        Map<DiscrepancyKind, DiscrepancyRecord> byKind = new EnumMap<>(DiscrepancyKind.class);
        for (DiscrepancyRecord d : report.discrepancies()) {
            byKind.put(d.kind(), d);
        }

        assertThat(byKind).containsOnlyKeys(
                DiscrepancyKind.MISSING_IN_LEDGER,
                DiscrepancyKind.MISSING_IN_PSP_FILE,
                DiscrepancyKind.AMOUNT_MISMATCH,
                DiscrepancyKind.STATUS_MISMATCH);

        // Verify details of each discrepancy kind
        DiscrepancyRecord missingInLedger = byKind.get(DiscrepancyKind.MISSING_IN_LEDGER);
        assertThat(missingInLedger.pspRef()).isEqualTo("psp_ghost_in_csv");
        assertThat(missingInLedger.actualAmt()).isEqualTo(8_800L);

        DiscrepancyRecord missingInPsp = byKind.get(DiscrepancyKind.MISSING_IN_PSP_FILE);
        assertThat(missingInPsp.pspRef()).isEqualTo("psp_missing_in_csv");
        assertThat(missingInPsp.paymentId()).isEqualTo(missingInPspId);
        assertThat(missingInPsp.expectedAmt()).isEqualTo(12_000L);

        DiscrepancyRecord amtMismatch = byKind.get(DiscrepancyKind.AMOUNT_MISMATCH);
        assertThat(amtMismatch.pspRef()).isEqualTo("psp_amount_diff");
        assertThat(amtMismatch.paymentId()).isEqualTo(amountMismatchId);
        assertThat(amtMismatch.expectedAmt()).isEqualTo(20_000L);
        assertThat(amtMismatch.actualAmt()).isEqualTo(19_500L);

        DiscrepancyRecord statusMismatch = byKind.get(DiscrepancyKind.STATUS_MISMATCH);
        assertThat(statusMismatch.pspRef()).isEqualTo("psp_status_diff");
        assertThat(statusMismatch.paymentId()).isEqualTo(statusMismatchId);
    }

    @Test
    @DisplayName("Reconciliation compares ledger receivable by currency and rejects duplicate or wrong-date rows")
    void ledgerCurrencyAndSettlementFileValidation() {
        UUID paymentId = insertPaymentWithLedger("psp_currency", "CAPTURED", 3_000L, 3_000L, 0L, true);
        jdbcClient.sql("UPDATE payments SET currency = 'USD' WHERE id = :id").param("id", paymentId).update();

        ReconRunReport report = reconciliationService.runReconciliation(LocalDate.of(2026, 10, 11), """
                psp_ref,amount,status,settled_at,currency
                psp_currency,3000,SETTLED,2026-10-11T10:00:00Z,INR
                """);
        assertThat(report.discrepancies()).singleElement()
                .satisfies(d -> assertThat(d.kind()).isEqualTo(DiscrepancyKind.CURRENCY_MISMATCH));

        assertThatThrownBy(() -> reconciliationService.runReconciliation(LocalDate.of(2026, 10, 11), """
                psp_ref,amount,status,settled_at
                psp_currency,3000,SETTLED,2026-10-11T10:00:00Z
                psp_currency,3000,SETTLED,2026-10-11T10:00:00Z
                """))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate PSP reference");

        assertThatThrownBy(() -> reconciliationService.runReconciliation(LocalDate.of(2026, 10, 11), """
                psp_ref,amount,status,settled_at
                psp_currency,3000,SETTLED,2026-10-12T00:00:00Z
                """))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("does not match requested date");
    }

    @Test
    @DisplayName("Settlement CSV honors quoted fields containing commas")
    void settlementCsvParsesQuotedCommaInPspReference() {
        ReconRunReport report = reconciliationService.runReconciliation(LocalDate.of(2026, 10, 11), """
                psp_ref,amount,status,settled_at,currency
                "psp,ghost",3000,SETTLED,2026-10-11T10:00:00Z,INR
                """);

        assertThat(report.discrepancies()).singleElement().satisfies(discrepancy -> {
            assertThat(discrepancy.kind()).isEqualTo(DiscrepancyKind.MISSING_IN_LEDGER);
            assertThat(discrepancy.pspRef()).isEqualTo("psp,ghost");
        });
    }

    @Test
    @DisplayName("Daily reconciliation matches capture and later refund movements on their own dates")
    void captureAndRefundAreReconciledOnTheirLedgerDates() {
        UUID paymentId = insertPaymentWithLedger("psp_cross_day", "PARTIALLY_REFUNDED", 25_000L, 25_000L, 0L, true);
        UUID refundTxnId = UuidV7.generate();
        jdbcClient.sql("INSERT INTO ledger_transactions (id, payment_id, kind, created_at) VALUES (:id, :paymentId, 'REFUND', '2026-10-12T02:00:00Z')")
                .param("id", refundTxnId).param("paymentId", paymentId).update();
        addReceivableEntry(refundTxnId, "CREDIT", 5_000L);

        ReconRunReport captureDay = reconciliationService.runReconciliation(LocalDate.of(2026, 10, 11), """
                psp_ref,amount,status,settled_at,currency
                psp_cross_day,25000,SETTLED,2026-10-11T10:00:00Z,INR
                """);
        assertThat(captureDay.matched()).isEqualTo(1);
        assertThat(captureDay.discrepancies()).isEmpty();

        ReconRunReport refundDay = reconciliationService.runReconciliation(LocalDate.of(2026, 10, 12), """
                psp_ref,amount,status,settled_at,currency
                psp_cross_day,-5000,SETTLED,2026-10-12T02:00:00Z,INR
                """);
        assertThat(refundDay.matched()).isEqualTo(1);
        assertThat(refundDay.discrepancies()).isEmpty();
    }

    private UUID insertPaymentWithLedger(
            String pspRef, String status, long amount, long capturedAmount, long refundedAmount, boolean withCaptureLedgerTxn) {
        UUID paymentId = UuidV7.generate();
        jdbcClient.sql("""
                INSERT INTO payments (id, psp_ref, status, amount, captured_amount, refunded_amount, updated_at)
                VALUES (:id, :pspRef, :status, :amount, :capturedAmount, :refundedAmount, '2026-10-11T10:00:00Z')
                """)
                .param("id", paymentId)
                .param("pspRef", pspRef)
                .param("status", status)
                .param("amount", amount)
                .param("capturedAmount", capturedAmount)
                .param("refundedAmount", refundedAmount)
                .update();

        if (withCaptureLedgerTxn) {
            UUID captureTxnId = UuidV7.generate();
            jdbcClient.sql("""
                    INSERT INTO ledger_transactions (id, payment_id, kind, created_at)
                    VALUES (:id, :paymentId, 'CAPTURE', '2026-10-11T10:00:00Z')
                    """)
                    .param("id", captureTxnId)
                    .param("paymentId", paymentId)
                    .update();
            addReceivableEntry(captureTxnId, "DEBIT", capturedAmount);
            if (refundedAmount > 0) {
                UUID refundTxnId = UuidV7.generate();
                jdbcClient.sql("INSERT INTO ledger_transactions (id, payment_id, kind, created_at) VALUES (:id, :paymentId, 'REFUND', '2026-10-11T10:01:00Z')")
                        .param("id", refundTxnId).param("paymentId", paymentId).update();
                addReceivableEntry(refundTxnId, "CREDIT", refundedAmount);
            }
        }
        return paymentId;
    }

    private void addReceivableEntry(UUID txnId, String direction, long amount) {
        if (amount == 0) return;
        for (var posting : java.util.List.of(
                Map.entry(UUID.fromString("00000000-0000-7000-8000-000000000001"), direction),
                Map.entry(UUID.fromString("00000000-0000-7000-8000-000000000002"), "DEBIT".equals(direction) ? "CREDIT" : "DEBIT"))) {
            jdbcClient.sql("INSERT INTO ledger_entries (id, txn_id, account_id, direction, amount) VALUES (:id, :txnId, :accountId, :direction, :amount)")
                    .param("id", UuidV7.generate()).param("txnId", txnId)
                    .param("accountId", posting.getKey()).param("direction", posting.getValue())
                    .param("amount", amount).update();
        }
    }
}

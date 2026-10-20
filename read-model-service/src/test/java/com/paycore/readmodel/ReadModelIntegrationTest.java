package com.paycore.readmodel;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import com.paycore.common.events.v1.PaymentAuthorized;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.events.v1.PayoutCreated;
import com.paycore.common.events.v1.RefundProcessed;
import com.paycore.common.ids.UuidV7;
import com.paycore.readmodel.ReadModelProjectorService.CursorPage;
import com.paycore.readmodel.ReadModelProjectorService.MerchantSummaryResponse;
import com.paycore.readmodel.ReadModelProjectorService.PaymentViewRow;
import com.paycore.readmodel.ReadModelProjectorService.RebuildReport;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
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
class ReadModelIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_readmodel_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.readmodel.kafka-listener-enabled", () -> "false");
        registry.add("paycore.security.admin-token", () -> "readmodel-test-admin");
    }

    @Autowired
    private ReadModelProjectorService projectorService;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TestRestTemplate restTemplate;

    private UUID merchantId;
    private String merchantApiKey;

    @BeforeEach
    void setUp() {
        jdbcClient.sql("TRUNCATE TABLE payment_view, merchant_daily_summary, processed_events, read_model_event_log, read_model_poison_events")
                .update();
        merchantId = UuidV7.generate();
        jdbcClient.sql("CREATE TABLE IF NOT EXISTS merchants (id UUID PRIMARY KEY, api_key_hash VARCHAR(64) NOT NULL UNIQUE)")
                .update();
        merchantApiKey = "pc_live_test_" + merchantId;
        jdbcClient.sql("INSERT INTO merchants (id, api_key_hash) VALUES (:id, :hash)")
                .param("id", merchantId)
                .param("hash", apiKeyHash(merchantApiKey))
                .update();
    }

    @Test
    @DisplayName("Malformed events are durably quarantined and duplicate poison payloads are idempotent")
    void malformedEventsAreQuarantined() {
        byte[] malformed = new byte[] {0x01, 0x02, (byte) 0xFF};

        projectorService.ingestEvent(malformed);
        projectorService.ingestEvent(malformed);

        Long quarantined = jdbcClient.sql("SELECT COUNT(*) FROM read_model_poison_events")
                .query(Long.class).single();
        assertThat(quarantined).isEqualTo(1L);
    }

    @Test
    @DisplayName("Merchant summary includes payouts and rebuild reproduces the payable balance")
    void merchantSummaryIncludesPayoutsAndRebuilds() {
        UUID paymentId = UuidV7.generate();
        Instant now = Instant.parse("2026-10-11T10:00:00Z");
        projectorService.ingestEvent(buildAuthorizedEvent(
                UuidV7.generate(), paymentId, merchantId, 1L, 10_000L, "psp-payout", now));
        projectorService.ingestEvent(buildCapturedEvent(
                UuidV7.generate(), paymentId, merchantId, 2L, 10_000L, 290L, now.plusSeconds(1)));

        UUID payoutId = UuidV7.generate();
        byte[] payoutEvent = PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setPaymentId(payoutId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(1)
                .setEventType("payout.created")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(now.plusSeconds(2).getEpochSecond()).build())
                .setPayout(PayoutCreated.newBuilder().setPayoutId(payoutId.toString())
                        .setAmount(9_710L).setCurrency("INR").setLedgerTxnId(UuidV7.generate().toString()))
                .build().toByteArray();
        projectorService.ingestEvent(payoutEvent);

        UUID usdPaymentId = UuidV7.generate();
        Instant usdAt = now.plusSeconds(3);
        projectorService.ingestEvent(PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString()).setPaymentId(usdPaymentId.toString())
                .setMerchantId(merchantId.toString()).setEventSeq(1).setEventType("payment.authorized")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(usdAt.getEpochSecond()).build())
                .setAuthorized(PaymentAuthorized.newBuilder().setAmount(5_000L).setCurrency("USD").setPspRef("psp-usd"))
                .build().toByteArray());
        projectorService.ingestEvent(PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString()).setPaymentId(usdPaymentId.toString())
                .setMerchantId(merchantId.toString()).setEventSeq(2).setEventType("payment.captured")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(usdAt.plusSeconds(1).getEpochSecond()).build())
                .setCaptured(PaymentCaptured.newBuilder().setCapturedAmount(5_000L).setFeeAmount(50L)
                        .setCurrency("USD").setPspRef("psp-usd").setLedgerTxnId(UuidV7.generate().toString()))
                .build().toByteArray());

        MerchantSummaryResponse before = projectorService.getMerchantSummary(merchantId);
        assertThat(before.currencies()).hasSize(2);
        assertThat(before.currencies()).filteredOn(summary -> summary.currency().equals("INR"))
                .singleElement().satisfies(summary -> {
                    assertThat(summary.totalPayouts()).isEqualTo(9_710L);
                    assertThat(summary.netMerchantAmount()).isZero();
                    assertThat(summary.dailyBreakdown()).singleElement()
                            .satisfies(day -> assertThat(day.payoutAmount()).isEqualTo(9_710L));
                });
        assertThat(before.currencies()).filteredOn(summary -> summary.currency().equals("USD"))
                .singleElement().satisfies(summary -> {
                    assertThat(summary.netMerchantAmount()).isEqualTo(4_950L);
                    assertThat(summary.totalPayouts()).isZero();
                });

        projectorService.rebuildFromScratch();
        assertThat(projectorService.getMerchantSummary(merchantId)).isEqualTo(before);
    }

    @Test
    @DisplayName("A different event claiming an occupied payment sequence is quarantined")
    void conflictingEventSequenceIsQuarantined() {
        UUID paymentId = UuidV7.generate();
        Instant now = Instant.parse("2026-10-11T10:00:00Z");
        projectorService.ingestEvent(buildAuthorizedEvent(
                UuidV7.generate(), paymentId, merchantId, 1L, 1_000L, "psp-conflict", now));
        projectorService.ingestEvent(buildAuthorizedEvent(
                UuidV7.generate(), paymentId, merchantId, 1L, 2_000L, "psp-conflict", now));

        Long quarantined = jdbcClient.sql("SELECT COUNT(*) FROM read_model_poison_events")
                .query(Long.class).single();
        assertThat(quarantined).isEqualTo(1L);
        assertThat(jdbcClient.sql("SELECT COUNT(*) FROM payment_view WHERE payment_id = :id AND amount = 1000")
                .param("id", paymentId).query(Long.class).single()).isEqualTo(1L);
    }

    @Test
    @DisplayName("1. Out-of-order & duplicate event_seq projection buffers gaps and drains strictly in order")
    void outOfOrderAndDuplicateEventsProjectedStrictlyInSequenceOrder() {
        UUID paymentId = UuidV7.generate();
        Instant now = Instant.parse("2026-10-11T10:00:00Z");

        byte[] seq1Authorized = buildAuthorizedEvent(UuidV7.generate(), paymentId, merchantId, 1L, 10_000L, "psp_ref_1", now);
        byte[] seq2Captured = buildCapturedEvent(UuidV7.generate(), paymentId, merchantId, 2L, 10_000L, 290L, now.plusSeconds(5));
        byte[] seq3Refund1 = buildRefundedEvent(UuidV7.generate(), paymentId, merchantId, 3L, 2_500L, 2_500L, 72L, now.plusSeconds(10));
        byte[] seq4Refund2 = buildRefundedEvent(UuidV7.generate(), paymentId, merchantId, 4L, 1_500L, 4_000L, 44L, now.plusSeconds(20));
        byte[] seq5FinalRefund = buildRefundedEvent(UuidV7.generate(), paymentId, merchantId, 5L, 6_000L, 10_000L, 174L, now.plusSeconds(30));

        // 1. Deliver seq 3 first (out-of-order gap: missing seq 1 and seq 2)
        projectorService.ingestEvent(seq3Refund1);
        assertThat(projectorService.listPayments(merchantId, null, 10).items()).isEmpty();

        // 2. Deliver seq 2 next (still missing seq 1)
        projectorService.ingestEvent(seq2Captured);
        // Also deliver duplicate seq 2
        projectorService.ingestEvent(seq2Captured);
        assertThat(projectorService.listPayments(merchantId, null, 10).items()).isEmpty();

        // 3. Deliver seq 1 -> gap is filled! Must automatically drain seq 1 -> seq 2 -> seq 3!
        projectorService.ingestEvent(seq1Authorized);

        List<PaymentViewRow> rowsAfterDrain = projectorService.listPayments(merchantId, null, 10).items();
        assertThat(rowsAfterDrain).hasSize(1);
        PaymentViewRow view = rowsAfterDrain.get(0);
        assertThat(view.status()).isEqualTo("PARTIALLY_REFUNDED");
        assertThat(view.amount()).isEqualTo(10_000L);
        assertThat(view.capturedAmount()).isEqualTo(10_000L);
        assertThat(view.feeAmount()).isEqualTo(218L);
        assertThat(view.refundedAmount()).isEqualTo(2_500L);
        assertThat(view.lastEventSeq()).isEqualTo(3L);

        // 4. Deliver seq 4 and duplicate seq 1 + seq 3
        projectorService.ingestEvent(seq4Refund2);
        assertThat(projectorService.listPayments(merchantId, null, 10).items().get(0).status())
                .isEqualTo("PARTIALLY_REFUNDED");
        projectorService.ingestEvent(seq5FinalRefund);
        assertThat(projectorService.listPayments(merchantId, null, 10).items().get(0).status())
                .isEqualTo("REFUNDED");
        projectorService.ingestEvent(seq1Authorized);
        projectorService.ingestEvent(seq3Refund1);

        MerchantSummaryResponse summary = projectorService.getMerchantSummary(merchantId);
        assertThat(summary.currencies()).singleElement().satisfies(currency -> {
            assertThat(currency.currency()).isEqualTo("INR");
            assertThat(currency.totalCaptured()).isEqualTo(10_000L);
            assertThat(currency.totalFees()).isZero();
            assertThat(currency.totalRefunded()).isEqualTo(10_000L);
            assertThat(currency.netMerchantAmount()).isZero();
            assertThat(currency.paymentCount()).isEqualTo(1L);
        });
    }

    @Test
    @DisplayName("2. Cursor pagination (UUIDv7) + Rebuild-from-Scratch matches live projection 100%")
    void cursorPaginationAndRebuildFromOffsetZero() {
        Instant baseTime = Instant.parse("2026-10-11T12:00:00Z");
        for (int i = 1; i <= 15; i++) {
            UUID paymentId = UuidV7.generate();
            long amt = i * 1_000L;
            long fee = (amt * 290L) / 10_000L;
            projectorService.ingestEvent(buildAuthorizedEvent(
                    UuidV7.generate(), paymentId, merchantId, 1L, amt, "psp_" + i, baseTime.plusSeconds(i)));
            projectorService.ingestEvent(buildCapturedEvent(
                    UuidV7.generate(), paymentId, merchantId, 2L, amt, fee, baseTime.plusSeconds(i + 1)));
            if (i % 3 == 0) {
                projectorService.ingestEvent(buildRefundedEvent(
                        UuidV7.generate(), paymentId, merchantId, 3L, 200L, 200L,
                        Math.floorDiv(fee * 200L, amt), baseTime.plusSeconds(i + 2)));
            }
        }

        // Paginate in pages of 5 via REST controller
        List<UUID> collectedIds = new ArrayList<>();
        UUID cursor = null;
        int pages = 0;
        while (true) {
            String url = "/v1/payments?limit=5" + (cursor != null ? "&cursor=" + cursor : "");
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-API-Key", merchantApiKey);
            ResponseEntity<CursorPage<PaymentViewRow>> pageResp = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    new ParameterizedTypeReference<>() {});
            assertThat(pageResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            CursorPage<PaymentViewRow> page = pageResp.getBody();
            assertThat(page).isNotNull();
            page.items().forEach(r -> collectedIds.add(r.paymentId()));
            pages++;
            if (!page.hasMore()) {
                break;
            }
            cursor = page.nextCursor();
        }

        assertThat(pages).isEqualTo(3);
        assertThat(collectedIds).hasSize(15).doesNotHaveDuplicates();

        // Capture live summary before rebuild
        MerchantSummaryResponse beforeRebuild = projectorService.getMerchantSummary(merchantId);

        // Trigger POST /v1/admin/read-model/rebuild (truncates projection tables and replays from offset 0)
        HttpHeaders adminHeaders = new HttpHeaders();
        adminHeaders.set("X-PayCore-Admin-Token", "readmodel-test-admin");
        ResponseEntity<RebuildReport> rebuildResp = restTemplate.exchange(
                "/v1/admin/read-model/rebuild", HttpMethod.POST,
                new HttpEntity<>(null, adminHeaders), RebuildReport.class);
        assertThat(rebuildResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        RebuildReport report = rebuildResp.getBody();
        assertThat(report).isNotNull();
        assertThat(report.paymentsProjected()).isEqualTo(15L);
        assertThat(report.eventsProcessed()).isEqualTo(35L); // 15 auth + 15 capture + 5 refunds

        // Verify post-rebuild summary matches pre-rebuild summary paise-for-paise
        MerchantSummaryResponse afterRebuild = projectorService.getMerchantSummary(merchantId);
        assertThat(afterRebuild).isEqualTo(beforeRebuild);
    }

    @Test
    @DisplayName("Merchant reads reject caller-supplied identity without a valid API key")
    void merchantReadsRejectSpoofedMerchantIdHeader() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Merchant-Id", merchantId.toString());
        ResponseEntity<String> response = restTemplate.exchange(
                "/v1/payments", HttpMethod.GET, new HttpEntity<>(headers), String.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    private String apiKeyHash(String rawKey) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(rawKey.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private byte[] buildAuthorizedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long amount, String pspRef, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("payment.authorized")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build())
                .setAuthorized(PaymentAuthorized.newBuilder()
                        .setAmount(amount)
                        .setCurrency("INR")
                        .setPspRef(pspRef)
                        .build())
                .build()
                .toByteArray();
    }

    private byte[] buildCapturedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long capturedAmt, long feeAmt, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("payment.captured")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build())
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(capturedAmt)
                        .setFeeAmount(feeAmt)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build()
                .toByteArray();
    }

    private byte[] buildRefundedEvent(
            UUID eventId, UUID paymentId, UUID merchantId, long seq, long refundAmt, long totalRefunded,
            long feeReversed, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(eventId.toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(seq)
                .setEventType("refund.processed")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build())
                .setRefunded(RefundProcessed.newBuilder()
                        .setRefundId(UuidV7.generate().toString())
                        .setRefundAmount(refundAmt)
                        .setTotalRefundedAmount(totalRefunded)
                        .setFeeReversedAmount(feeReversed)
                        .setCurrency("INR")
                        .setLedgerTxnId(UuidV7.generate().toString())
                        .build())
                .build()
                .toByteArray();
    }
}

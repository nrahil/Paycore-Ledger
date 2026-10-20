package com.paycore.readmodel;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.events.v1.PaymentEvent;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * CQRS Read Model Projector & Query Engine (FR11, Section 5.1, Section 7.9).
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Applies events strictly in per-payment gapless {@code event_seq = last_event_seq + 1} order</li>
 *   <li>Ignores duplicate events via {@code processed_events} and {@code event_seq <= last_event_seq}</li>
 *   <li>Buffers out-of-order events on a gap and drains them automatically once the gap is filled</li>
 *   <li>Supports deterministic rebuild-from-scratch from offset 0 verified against core ledger totals</li>
 * </ul>
 */
@Service
public class ReadModelProjectorService {

    public static final String CONSUMER_NAME = "read-model-service";
    private static final Logger log = LoggerFactory.getLogger(ReadModelProjectorService.class);

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;

    public ReadModelProjectorService(JdbcClient jdbcClient, ClockProvider clockProvider) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
    }

    @KafkaListener(
            topics = "payments.events",
            groupId = "read-model-service-group",
            autoStartup = "${paycore.readmodel.kafka-listener-enabled:false}")
    public void onKafkaPaymentEvent(byte[] rawPayload) {
        ingestEvent(rawPayload);
    }

    /**
     * Ingests a raw Protobuf {@link PaymentEvent}, records it in the replay/gap buffer,
     * and applies all contiguous {@code last_event_seq + 1} events for that aggregate.
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void ingestEvent(byte[] rawPayload) {
        PaymentEvent event;
        try {
            event = PaymentEvent.parseFrom(rawPayload);
        } catch (Exception e) {
            quarantinePoisonEvent(rawPayload, e.getMessage());
            log.warn("Read model quarantined malformed Protobuf payload: {}", e.getMessage());
            return;
        }

        UUID eventId;
        UUID paymentId;
        try {
            eventId = UUID.fromString(event.getEventId());
            paymentId = UUID.fromString(event.getPaymentId());
            UUID.fromString(event.getMerchantId());
            if (event.getEventSeq() <= 0) throw new IllegalArgumentException("event_seq must be positive");
        } catch (RuntimeException invalidMetadata) {
            quarantinePoisonEvent(rawPayload, invalidMetadata.getMessage());
            return;
        }
        long eventSeq = event.getEventSeq();

        // A SELECT FOR UPDATE cannot lock a missing payment_view row; this lock serializes the first event too.
        jdbcClient.sql("SELECT pg_advisory_xact_lock(hashtextextended(:lockKey, 0))")
                .param("lockKey", CONSUMER_NAME + ":" + paymentId)
                .query((rs, rowNum) -> 1)
                .single();

        if (isAlreadyProcessed(eventId)) {
            return;
        }

        // Persist in event log for gap buffering and offset-0 rebuilds
        int inserted = jdbcClient.sql("""
                INSERT INTO read_model_event_log (payment_id, event_seq, event_id, raw_payload, received_at)
                VALUES (:paymentId, :eventSeq, :eventId, :rawPayload, :receivedAt)
                ON CONFLICT (payment_id, event_seq) DO NOTHING
                """)
                .param("paymentId", paymentId)
                .param("eventSeq", eventSeq)
                .param("eventId", eventId)
                .param("rawPayload", rawPayload)
                .param("receivedAt", Timestamp.from(clockProvider.now()))
                .update();

        if (inserted == 0) {
            UUID existingEventId = jdbcClient.sql("""
                            SELECT event_id FROM read_model_event_log
                            WHERE payment_id = :paymentId AND event_seq = :eventSeq
                            """)
                    .param("paymentId", paymentId).param("eventSeq", eventSeq)
                    .query(UUID.class).single();
            byte[] existingPayload = jdbcClient.sql("""
                            SELECT raw_payload FROM read_model_event_log
                            WHERE payment_id = :paymentId AND event_seq = :eventSeq
                            """)
                    .param("paymentId", paymentId).param("eventSeq", eventSeq)
                    .query((rs, rowNum) -> rs.getBytes("raw_payload")).single();
            if (!existingEventId.equals(eventId) || !java.security.MessageDigest.isEqual(existingPayload, rawPayload)) {
                quarantinePoisonEvent(rawPayload, "Conflicting event_id for payment/event_seq " + paymentId + "/" + eventSeq);
            }
            return;
        }

        drainContiguousSequenceForPayment(paymentId);
    }

    private void quarantinePoisonEvent(byte[] rawPayload, String reason) {
        String hash;
        try {
            hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(rawPayload));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
        jdbcClient.sql("""
                INSERT INTO read_model_poison_events (id, payload_sha256, raw_payload, reason, received_at)
                VALUES (:id, :hash, :payload, :reason, :receivedAt)
                ON CONFLICT (payload_sha256) DO NOTHING
                """)
                .param("id", UUID.randomUUID())
                .param("hash", hash)
                .param("payload", rawPayload)
                .param("reason", reason == null ? "Invalid payment event" : reason)
                .param("receivedAt", Timestamp.from(clockProvider.now()))
                .update();
    }

    private void drainContiguousSequenceForPayment(UUID paymentId) {
        long lastSeq = jdbcClient.sql("SELECT last_event_seq FROM payment_view WHERE payment_id = :id FOR UPDATE")
                .param("id", paymentId)
                .query(Long.class)
                .optional()
                .orElse(0L);

        while (true) {
            long expectedNextSeq = lastSeq + 1;
            Optional<byte[]> nextRawOpt = jdbcClient.sql("""
                    SELECT raw_payload
                    FROM read_model_event_log
                    WHERE payment_id = :paymentId AND event_seq = :eventSeq
                    """)
                    .param("paymentId", paymentId)
                    .param("eventSeq", expectedNextSeq)
                    .query((rs, rowNum) -> rs.getBytes("raw_payload"))
                    .optional();

            if (nextRawOpt.isEmpty()) {
                // Gap detected or caught up: stop and wait for missing event_seq
                break;
            }

            try {
                PaymentEvent nextEvent = PaymentEvent.parseFrom(nextRawOpt.get());
                applyOrderedEvent(nextEvent);
                markProcessed(UUID.fromString(nextEvent.getEventId()));
                lastSeq = expectedNextSeq;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to apply buffered event seq " + expectedNextSeq, e);
            }
        }
    }

    private void applyOrderedEvent(PaymentEvent event) {
        UUID paymentId = UUID.fromString(event.getPaymentId());
        UUID merchantId = UUID.fromString(event.getMerchantId());
        long eventSeq = event.getEventSeq();
        Instant occurredAt = event.hasOccurredAt()
                ? Instant.ofEpochSecond(event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos())
                : clockProvider.now();
        Timestamp occurredTs = Timestamp.from(occurredAt);
        LocalDate day = occurredAt.atZone(ZoneOffset.UTC).toLocalDate();

        switch (event.getPayloadCase()) {
            case AUTHORIZED -> {
                long amount = event.getAuthorized().getAmount();
                String currency = event.getAuthorized().getCurrency();
                String pspRef = event.getAuthorized().getPspRef();
                jdbcClient.sql("""
                        INSERT INTO payment_view (
                            payment_id, merchant_id, status, amount, captured_amount,
                            refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                        ) VALUES (
                            :paymentId, :merchantId, 'AUTHORIZED', :amount, 0,
                            0, 0, :currency, :pspRef, :eventSeq, :updatedAt
                        )
                        ON CONFLICT (payment_id) DO UPDATE
                        SET status = 'AUTHORIZED',
                            amount = EXCLUDED.amount,
                            psp_ref = EXCLUDED.psp_ref,
                            last_event_seq = EXCLUDED.last_event_seq,
                            updated_at = EXCLUDED.updated_at
                        WHERE payment_view.last_event_seq = :eventSeq - 1
                        """)
                        .param("paymentId", paymentId)
                        .param("merchantId", merchantId)
                        .param("amount", amount)
                        .param("currency", currency)
                        .param("pspRef", pspRef)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .update();
            }
            case FAILED -> {
                long amount = event.getFailed().getAmount();
                String currency = event.getFailed().getCurrency();
                String pspRef = event.getFailed().getPspRef();
                jdbcClient.sql("""
                        INSERT INTO payment_view (
                            payment_id, merchant_id, status, amount, captured_amount,
                            refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                        ) VALUES (
                            :paymentId, :merchantId, 'FAILED', :amount, 0,
                            0, 0, :currency, :pspRef, :eventSeq, :updatedAt
                        )
                        ON CONFLICT (payment_id) DO UPDATE
                        SET status = 'FAILED',
                            last_event_seq = EXCLUDED.last_event_seq,
                            updated_at = EXCLUDED.updated_at
                        WHERE payment_view.last_event_seq = :eventSeq - 1
                        """)
                        .param("paymentId", paymentId)
                        .param("merchantId", merchantId)
                        .param("amount", amount)
                        .param("currency", currency)
                        .param("pspRef", pspRef)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .update();
            }
            case CAPTURED -> {
                long capturedAmount = event.getCaptured().getCapturedAmount();
                long feeAmount = event.getCaptured().getFeeAmount();
                jdbcClient.sql("""
                        UPDATE payment_view
                        SET status = 'CAPTURED',
                            captured_amount = :capturedAmount,
                            fee_amount = :feeAmount,
                            last_event_seq = :eventSeq,
                            updated_at = :updatedAt
                        WHERE payment_id = :paymentId
                          AND last_event_seq = :eventSeq - 1
                        """)
                        .param("capturedAmount", capturedAmount)
                        .param("feeAmount", feeAmount)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .param("paymentId", paymentId)
                        .update();

                jdbcClient.sql("""
                        INSERT INTO merchant_daily_summary (merchant_id, day, currency, captured, refunded, fees, payment_count)
                        VALUES (:merchantId, :day, :currency, :captured, 0, :fees, 1)
                        ON CONFLICT (merchant_id, day, currency) DO UPDATE
                        SET captured = merchant_daily_summary.captured + EXCLUDED.captured,
                            fees = merchant_daily_summary.fees + EXCLUDED.fees,
                            payment_count = merchant_daily_summary.payment_count + 1
                        """)
                        .param("merchantId", merchantId)
                        .param("day", Date.valueOf(day))
                        .param("currency", event.getCaptured().getCurrency())
                        .param("captured", capturedAmount)
                        .param("fees", feeAmount)
                        .update();
            }
            case REFUNDED -> {
                long refundAmount = event.getRefunded().getRefundAmount();
                long feeReversed = event.getRefunded().getFeeReversedAmount();
                long totalRefunded = event.getRefunded().getTotalRefundedAmount();
                jdbcClient.sql("""
                        UPDATE payment_view
                        SET status = CASE
                                WHEN :totalRefunded >= captured_amount THEN 'REFUNDED'
                                ELSE 'PARTIALLY_REFUNDED'
                            END,
                            refunded_amount = :totalRefunded,
                            fee_amount = GREATEST(0, fee_amount - :feeReversed),
                            last_event_seq = :eventSeq,
                            updated_at = :updatedAt
                        WHERE payment_id = :paymentId
                          AND last_event_seq = :eventSeq - 1
                        """)
                        .param("totalRefunded", totalRefunded)
                        .param("feeReversed", feeReversed)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .param("paymentId", paymentId)
                        .update();

                jdbcClient.sql("""
                        INSERT INTO merchant_daily_summary (merchant_id, day, currency, captured, refunded, fees, payment_count)
                        VALUES (:merchantId, :day, :currency, 0, :refunded, :negativeFeeReversal, 0)
                        ON CONFLICT (merchant_id, day, currency) DO UPDATE
                        SET refunded = merchant_daily_summary.refunded + EXCLUDED.refunded,
                            fees = merchant_daily_summary.fees + EXCLUDED.fees
                        """)
                        .param("merchantId", merchantId)
                        .param("day", Date.valueOf(day))
                        .param("currency", event.getRefunded().getCurrency())
                        .param("refunded", refundAmount)
                        .param("negativeFeeReversal", -feeReversed)
                        .update();
            }
            case PAYOUT -> {
                jdbcClient.sql("""
                        INSERT INTO merchant_daily_summary (merchant_id, day, currency, payout_amount)
                        VALUES (:merchantId, :day, :currency, :amount)
                        ON CONFLICT (merchant_id, day, currency) DO UPDATE
                        SET payout_amount = merchant_daily_summary.payout_amount + EXCLUDED.payout_amount
                        """)
                        .param("merchantId", merchantId)
                        .param("day", Date.valueOf(day))
                        .param("currency", event.getPayout().getCurrency())
                        .param("amount", event.getPayout().getAmount())
                        .update();
            }
            default -> {}
        }
    }

    /**
     * Rebuilds the entire CQRS read model projection from scratch from offset 0 (Section 2 FR11 & Section 7.9).
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public RebuildReport rebuildFromScratch() {
        jdbcClient.sql("TRUNCATE TABLE payment_view, merchant_daily_summary, processed_events").update();

        List<UUID> distinctPayments = jdbcClient.sql("""
                SELECT DISTINCT payment_id FROM read_model_event_log
                """)
                .query(UUID.class)
                .list();

        for (UUID paymentId : distinctPayments) {
            drainContiguousSequenceForPayment(paymentId);
        }

        long eventsProcessed = jdbcClient.sql(
                        "SELECT COUNT(*) FROM processed_events WHERE consumer = :c")
                .param("c", CONSUMER_NAME)
                .query(Long.class)
                .single();
        long paymentsProjected = jdbcClient.sql("SELECT COUNT(*) FROM payment_view")
                .query(Long.class)
                .single();
        long totalCaptured = jdbcClient.sql("SELECT COALESCE(SUM(captured_amount), 0) FROM payment_view")
                .query(Long.class)
                .single();
        long totalRefunded = jdbcClient.sql("SELECT COALESCE(SUM(refunded_amount), 0) FROM payment_view")
                .query(Long.class)
                .single();
        long totalFees = jdbcClient.sql("SELECT COALESCE(SUM(fee_amount), 0) FROM payment_view")
                .query(Long.class)
                .single();

        return new RebuildReport(
                eventsProcessed,
                paymentsProjected,
                totalCaptured,
                totalRefunded,
                totalFees,
                clockProvider.now());
    }

    @Transactional(readOnly = true)
    public CursorPage<PaymentViewRow> listPayments(UUID merchantId, UUID cursor, int limit) {
        int clampedLimit = Math.max(1, Math.min(limit, 100));
        List<PaymentViewRow> rows;
        if (cursor == null) {
            rows = jdbcClient.sql("""
                    SELECT payment_id, merchant_id, status, amount, captured_amount,
                           refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                    FROM payment_view
                    WHERE merchant_id = :merchantId
                    ORDER BY payment_id DESC
                    LIMIT :limitPlusOne
                    """)
                    .param("merchantId", merchantId)
                    .param("limitPlusOne", clampedLimit + 1)
                    .query(this::mapPaymentView)
                    .list();
        } else {
            rows = jdbcClient.sql("""
                    SELECT payment_id, merchant_id, status, amount, captured_amount,
                           refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                    FROM payment_view
                    WHERE merchant_id = :merchantId
                      AND payment_id < :cursor
                    ORDER BY payment_id DESC
                    LIMIT :limitPlusOne
                    """)
                    .param("merchantId", merchantId)
                    .param("cursor", cursor)
                    .param("limitPlusOne", clampedLimit + 1)
                    .query(this::mapPaymentView)
                    .list();
        }

        boolean hasMore = rows.size() > clampedLimit;
        List<PaymentViewRow> items = hasMore ? rows.subList(0, clampedLimit) : rows;
        UUID nextCursor = hasMore ? items.get(items.size() - 1).paymentId() : null;
        return new CursorPage<>(items, nextCursor, hasMore);
    }

    @Transactional(readOnly = true)
    public MerchantSummaryResponse getMerchantSummary(UUID merchantId) {
        record Totals(String currency, long captured, long refunded, long fees, long paymentCount) {}
        Map<String, Totals> paymentTotals = jdbcClient.sql("""
                SELECT
                    currency,
                    COALESCE(SUM(captured_amount), 0) AS total_captured,
                    COALESCE(SUM(refunded_amount), 0) AS total_refunded,
                    COALESCE(SUM(fee_amount), 0)      AS total_fees,
                    COUNT(*) FILTER (WHERE captured_amount > 0) AS payment_count
                FROM payment_view
                WHERE merchant_id = :merchantId
                GROUP BY currency
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new Totals(
                        rs.getString("currency"), rs.getLong("total_captured"),
                        rs.getLong("total_refunded"), rs.getLong("total_fees"),
                        rs.getLong("payment_count")))
                .list().stream().collect(java.util.stream.Collectors.toMap(Totals::currency, t -> t));

        List<DailySummaryRow> daily = jdbcClient.sql("""
                SELECT merchant_id, day, currency, captured, refunded, fees, payout_amount, payment_count
                FROM merchant_daily_summary
                WHERE merchant_id = :merchantId
                ORDER BY currency, day DESC
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new DailySummaryRow(
                        rs.getObject("merchant_id", UUID.class),
                        rs.getDate("day").toLocalDate(),
                        rs.getString("currency"),
                        rs.getLong("captured"),
                        rs.getLong("refunded"),
                        rs.getLong("fees"),
                        rs.getLong("payout_amount"),
                        rs.getLong("payment_count")))
                .list();

        java.util.Set<String> currencies = new java.util.TreeSet<>(paymentTotals.keySet());
        daily.forEach(row -> currencies.add(row.currency()));
        List<CurrencySummary> summaries = currencies.stream().map(currency -> {
            Totals totals = paymentTotals.getOrDefault(currency, new Totals(currency, 0, 0, 0, 0));
            List<DailySummaryRow> currencyDaily = daily.stream()
                    .filter(row -> row.currency().equals(currency)).toList();
            long payouts = currencyDaily.stream().mapToLong(DailySummaryRow::payoutAmount).sum();
            return new CurrencySummary(currency, totals.captured(), totals.refunded(), totals.fees(), payouts,
                    totals.captured() - totals.refunded() - totals.fees() - payouts, totals.paymentCount(), currencyDaily);
        }).toList();
        return new MerchantSummaryResponse(merchantId, summaries);
    }

    private boolean isAlreadyProcessed(UUID eventId) {
        Integer cnt = jdbcClient.sql("""
                SELECT COUNT(*) FROM processed_events
                WHERE consumer = :consumer AND event_id = :eventId
                """)
                .param("consumer", CONSUMER_NAME)
                .param("eventId", eventId)
                .query(Integer.class)
                .single();
        return cnt != null && cnt > 0;
    }

    private void markProcessed(UUID eventId) {
        jdbcClient.sql("""
                INSERT INTO processed_events (consumer, event_id, processed_at)
                VALUES (:consumer, :eventId, :processedAt)
                ON CONFLICT (consumer, event_id) DO NOTHING
                """)
                .param("consumer", CONSUMER_NAME)
                .param("eventId", eventId)
                .param("processedAt", Timestamp.from(clockProvider.now()))
                .update();
    }

    private PaymentViewRow mapPaymentView(ResultSet rs, int rowNum) throws SQLException {
        return new PaymentViewRow(
                rs.getObject("payment_id", UUID.class),
                rs.getObject("merchant_id", UUID.class),
                rs.getString("status"),
                rs.getLong("amount"),
                rs.getLong("captured_amount"),
                rs.getLong("refunded_amount"),
                rs.getLong("fee_amount"),
                rs.getString("currency"),
                rs.getString("psp_ref"),
                rs.getLong("last_event_seq"),
                rs.getTimestamp("updated_at").toInstant());
    }

    public record PaymentViewRow(
            UUID paymentId,
            UUID merchantId,
            String status,
            long amount,
            long capturedAmount,
            long refundedAmount,
            long feeAmount,
            String currency,
            String pspRef,
            long lastEventSeq,
            Instant updatedAt) {}

    public record CursorPage<T>(List<T> items, UUID nextCursor, boolean hasMore) {}

    public record DailySummaryRow(
            UUID merchantId,
            LocalDate day,
            String currency,
            long captured,
            long refunded,
            long fees,
            long payoutAmount,
            long paymentCount) {}

    public record CurrencySummary(
            String currency,
            long totalCaptured,
            long totalRefunded,
            long totalFees,
            long totalPayouts,
            long netMerchantAmount,
            long paymentCount,
            List<DailySummaryRow> dailyBreakdown) {}

    public record MerchantSummaryResponse(UUID merchantId, List<CurrencySummary> currencies) {}

    public record RebuildReport(
            long eventsProcessed,
            long paymentsProjected,
            long totalCaptured,
            long totalRefunded,
            long totalFees,
            Instant rebuiltAt) {}
}

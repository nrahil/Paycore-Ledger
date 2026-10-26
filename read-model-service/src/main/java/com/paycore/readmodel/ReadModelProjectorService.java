package com.paycore.readmodel;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.events.v1.PaymentEvent;
import java.sql.Timestamp;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * CQRS Read Model Event Projector (FR11, Section 5.1, Section 7.9).
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Applies events strictly in per-payment gapless {@code event_seq = last_event_seq + 1} order</li>
 *   <li>Ignores duplicate events via {@code processed_events} and {@code event_seq <= last_event_seq}</li>
 *   <li>Buffers out-of-order events on a gap and drains them automatically once the gap is filled</li>
 *   <li>Retains durable ordered events so {@link ReadModelRebuildService} can deterministically rebuild</li>
 * </ul>
 */
@Service
public class ReadModelProjectorService {

    public static final String CONSUMER_NAME = "read-model-service";
    private static final Logger log = LoggerFactory.getLogger(ReadModelProjectorService.class);

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private final ReadModelProjectionWriter projectionWriter;

    public ReadModelProjectorService(
            JdbcClient jdbcClient, ClockProvider clockProvider, ReadModelProjectionWriter projectionWriter) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.projectionWriter = projectionWriter;
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
                projectionWriter.apply(nextEvent);
                markProcessed(UUID.fromString(nextEvent.getEventId()));
                lastSeq = expectedNextSeq;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to apply buffered event seq " + expectedNextSeq, e);
            }
        }
    }



    /** Replays logged payment events in per-payment sequence order inside the caller's transaction. */
    void replayStoredEventsInOrder() {
        List<UUID> distinctPayments = jdbcClient.sql("""
                SELECT DISTINCT payment_id FROM read_model_event_log
                """)
                .query(UUID.class)
                .list();
        for (UUID paymentId : distinctPayments) {
            drainContiguousSequenceForPayment(paymentId);
        }
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

}

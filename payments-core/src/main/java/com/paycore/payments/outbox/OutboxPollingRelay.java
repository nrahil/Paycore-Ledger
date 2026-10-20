package com.paycore.payments.outbox;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.tracing.TraceHeaders;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Outbox Relay v1: Polling with {@code FOR UPDATE SKIP LOCKED} (Section 4.2 #5, 5.1, 6.2, 7.7).
 *
 * <p>Claims unpublished rows from {@code outbox_events} using {@code FOR UPDATE SKIP LOCKED}
 * so multiple {@code payments-core} replicas never contend on the same batch, publishes
 * Protobuf {@code PaymentEvent} bytes to {@code payments.events} keyed by {@code payment_id},
 * propagates W3C {@code traceparent} in Kafka headers, and stamps {@code published_at}.
 */
@Component
public class OutboxPollingRelay {

    public static final String PAYMENTS_EVENTS_TOPIC = "payments.events";
    private static final Logger log = LoggerFactory.getLogger(OutboxPollingRelay.class);
    private static final TypeReference<Map<String, String>> MAP_TYPE = new TypeReference<>() {};

    private final JdbcClient jdbcClient;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;
    private final boolean relayEnabled;
    private final int batchSize;
    private final TransactionTemplate transactionTemplate;
    private final int leaseSeconds;

    private volatile Consumer<Integer> crashMidBatchHook = index -> {};
    private volatile Consumer<OutboxRow> customPublishSink;

    public OutboxPollingRelay(
            JdbcClient jdbcClient,
            @Autowired(required = false) KafkaTemplate<String, byte[]> kafkaTemplate,
            ObjectMapper objectMapper,
            ClockProvider clockProvider,
            PlatformTransactionManager transactionManager,
            @Value("${paycore.outbox.relay-enabled:false}") boolean relayEnabled,
            @Value("${paycore.outbox.batch-size:100}") int batchSize,
            @Value("${paycore.outbox.lease-seconds:30}") int leaseSeconds) {
        this.jdbcClient = jdbcClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
        this.relayEnabled = relayEnabled;
        this.batchSize = batchSize;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.leaseSeconds = Math.max(1, leaseSeconds);
    }

    @Scheduled(fixedDelayString = "${paycore.outbox.poll-interval-ms:250}")
    public void scheduledRelayTick() {
        if (!relayEnabled) {
            return;
        }
        try {
            relayNextBatch();
        } catch (Exception e) {
            log.warn("Outbox polling relay tick encountered error (will retry next tick): {}", e.getMessage());
        }
    }

    /**
     * Claims a batch in a short transaction, publishes outside the transaction, then acknowledges each row.
     * publishes each to Redpanda/Kafka keyed by {@code aggregate_id} ({@code payment_id}),
     * and marks {@code published_at}.
     */
    public int relayNextBatch() {
        UUID claimToken = UUID.randomUUID();
        List<OutboxRow> rows = transactionTemplate.execute(status -> jdbcClient.sql("""
                        WITH candidates AS (
                            SELECT current.id, current.created_at
                            FROM outbox_events current
                            WHERE current.published_at IS NULL
                              AND current.next_attempt_at <= NOW()
                              AND (current.lease_until IS NULL OR current.lease_until <= NOW())
                              AND NOT EXISTS (
                                  SELECT 1 FROM outbox_events previous
                                  WHERE previous.aggregate_id = current.aggregate_id
                                    AND previous.event_seq < current.event_seq
                                    AND previous.published_at IS NULL
                              )
                            ORDER BY current.created_at, current.id
                            LIMIT :batchSize
                            FOR UPDATE OF current SKIP LOCKED
                        )
                        UPDATE outbox_events target
                        SET claim_token = :claimToken,
                            lease_until = NOW() + (:leaseSeconds * INTERVAL '1 second')
                        FROM candidates
                        WHERE target.id = candidates.id AND target.created_at = candidates.created_at
                        RETURNING target.id, target.aggregate_id, target.event_seq, target.event_type,
                                  target.schema_version, target.payload,
                                  target.trace_context::text AS trace_json, target.created_at
                        """)
                .param("batchSize", batchSize)
                .param("claimToken", claimToken)
                .param("leaseSeconds", leaseSeconds)
                .query((rs, rowNum) -> new OutboxRow(
                        rs.getObject("id", UUID.class),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getLong("event_seq"),
                        rs.getString("event_type"),
                        rs.getInt("schema_version"),
                        rs.getBytes("payload"),
                        parseTraceContext(rs.getString("trace_json")),
                        rs.getTimestamp("created_at").toInstant()))
                .list());

        if (rows.isEmpty()) {
            return 0;
        }

        int publishedCount = 0;
        for (int i = 0; i < rows.size(); i++) {
            OutboxRow row = rows.get(i);
            try {
                publishToBus(row);
            } catch (RuntimeException failure) {
                // An expired lease recovers process crashes; ordinary broker errors are released immediately.
                jdbcClient.sql("""
                        UPDATE outbox_events
                        SET claim_token = NULL, lease_until = NULL,
                            attempt_count = attempt_count + 1,
                            next_attempt_at = NOW() + (LEAST(300, POWER(2, LEAST(attempt_count, 8))) * INTERVAL '1 second'),
                            last_error = :error
                        WHERE id = :id AND created_at = :createdAt AND claim_token = :claimToken
                        """)
                        .param("error", truncate(failure.getMessage(), 2000))
                        .param("id", row.id())
                        .param("createdAt", Timestamp.from(row.createdAt()))
                        .param("claimToken", claimToken)
                        .update();
                releaseUnprocessed(rows.subList(i + 1, rows.size()), claimToken);
                throw failure;
            }
            // This hook deliberately runs after the external publish but before acknowledgement to model a hard crash.
            crashMidBatchHook.accept(i);

            int acknowledged = jdbcClient.sql("""
                    UPDATE outbox_events
                    SET published_at = :publishedAt, claim_token = NULL, lease_until = NULL, last_error = NULL
                    WHERE id = :id AND created_at = :createdAt AND claim_token = :claimToken
                    """)
                    .param("publishedAt", Timestamp.from(clockProvider.now()))
                    .param("id", row.id())
                    .param("createdAt", Timestamp.from(row.createdAt()))
                    .param("claimToken", claimToken)
                    .update();
            if (acknowledged == 1) publishedCount++;
        }

        return publishedCount;
    }

    private void releaseUnprocessed(List<OutboxRow> rows, UUID claimToken) {
        for (OutboxRow row : rows) {
            jdbcClient.sql("UPDATE outbox_events SET claim_token = NULL, lease_until = NULL WHERE id = :id AND created_at = :createdAt AND claim_token = :claimToken")
                    .param("id", row.id()).param("createdAt", Timestamp.from(row.createdAt()))
                    .param("claimToken", claimToken).update();
        }
    }

    private String truncate(String value, int maxLength) {
        if (value == null) return "Unknown Kafka publish error";
        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    private void publishToBus(OutboxRow row) {
        Consumer<OutboxRow> sink = this.customPublishSink;
        if (sink != null) {
            sink.accept(row);
            return;
        }

        if (kafkaTemplate == null) {
            throw new IllegalStateException("KafkaTemplate is not configured and no customPublishSink is set");
        }

        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                PAYMENTS_EVENTS_TOPIC,
                null,
                row.aggregateId().toString(), // Keyed by payment_id so per-payment events stay strictly ordered
                row.payload());

        record.headers().add(new RecordHeader("event_id", row.id().toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("event_type", row.eventType().getBytes(StandardCharsets.UTF_8)));
        String traceparent = row.traceContext().get(TraceHeaders.TRACEPARENT);
        if (traceparent != null) {
            record.headers().add(
                    new RecordHeader(TraceHeaders.TRACEPARENT, traceparent.getBytes(StandardCharsets.UTF_8)));
        }

        try {
            kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish outbox event " + row.id() + " to Kafka", e);
        }
    }

    private Map<String, String> parseTraceContext(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    public void setCrashMidBatchHook(Consumer<Integer> crashMidBatchHook) {
        this.crashMidBatchHook = crashMidBatchHook != null ? crashMidBatchHook : idx -> {};
    }

    public void setCustomPublishSink(Consumer<OutboxRow> customPublishSink) {
        this.customPublishSink = customPublishSink;
    }

    public record OutboxRow(
            UUID id,
            UUID aggregateId,
            long eventSeq,
            String eventType,
            int schemaVersion,
            byte[] payload,
            Map<String, String> traceContext,
            Instant createdAt) {}
}

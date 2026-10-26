package com.paycore.webhook;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Owns webhook delivery history and processed-event persistence. */
@Repository
public class WebhookDeliveryRepository {

    private static final String DELIVERY_COLUMNS = """
            id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
            last_status_code, last_error, target_url, payload_json, traceparent, created_at
            """;

    private final JdbcClient jdbcClient;

    public WebhookDeliveryRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void insert(DeliveryRecord row) {
        jdbcClient.sql("""
                INSERT INTO webhook_deliveries (
                    id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                    last_status_code, last_error, target_url, payload_json, traceparent, created_at
                ) VALUES (
                    :id, :eventId, :merchantId, :attempt, :status, :retryTopic, :nextAttemptAt,
                    :lastStatusCode, :lastError, :targetUrl, :payloadJson, :traceparent, :createdAt
                )
                """)
                .param("id", row.id())
                .param("eventId", row.eventId())
                .param("merchantId", row.merchantId())
                .param("attempt", row.attempt())
                .param("status", row.status())
                .param("retryTopic", row.retryTopic())
                .param("nextAttemptAt", row.nextAttemptAt() == null ? null : Timestamp.from(row.nextAttemptAt()))
                .param("lastStatusCode", row.lastStatusCode())
                .param("lastError", row.lastError())
                .param("targetUrl", row.targetUrl())
                .param("payloadJson", row.payloadJson())
                .param("traceparent", row.traceparent())
                .param("createdAt", Timestamp.from(row.createdAt()))
                .update();
    }

    public boolean isProcessed(UUID eventId, String consumer) {
        Integer count = jdbcClient.sql("""
                        SELECT COUNT(*) FROM processed_events
                        WHERE consumer = :consumer AND event_id = :eventId
                        """)
                .param("consumer", consumer)
                .param("eventId", eventId)
                .query(Integer.class)
                .single();
        return count != null && count > 0;
    }

    public void markProcessed(UUID eventId, String consumer, Instant processedAt) {
        jdbcClient.sql("""
                INSERT INTO processed_events (consumer, event_id, processed_at)
                VALUES (:consumer, :eventId, :processedAt)
                ON CONFLICT (consumer, event_id) DO NOTHING
                """)
                .param("consumer", consumer)
                .param("eventId", eventId)
                .param("processedAt", Timestamp.from(processedAt))
                .update();
    }

    public int countAttempts(UUID eventId) {
        Integer count = jdbcClient.sql("SELECT COUNT(*) FROM webhook_deliveries WHERE event_id = :eventId")
                .param("eventId", eventId)
                .query(Integer.class)
                .single();
        return count == null ? 0 : count;
    }

    public boolean hasLaterAttempt(UUID eventId, int attempt) {
        return jdbcClient.sql("""
                        SELECT EXISTS (
                            SELECT 1 FROM webhook_deliveries WHERE event_id = :eventId AND attempt > :attempt
                        )
                        """)
                .param("eventId", eventId)
                .param("attempt", attempt)
                .query(Boolean.class)
                .single();
    }

    public Optional<DeliveryRecord> findById(UUID id) {
        return jdbcClient.sql("SELECT " + DELIVERY_COLUMNS + " FROM webhook_deliveries WHERE id = :id")
                .param("id", id)
                .query(WebhookDeliveryRepository::mapRow)
                .optional();
    }

    public boolean existsForMerchant(UUID id, UUID merchantId) {
        return jdbcClient.sql("""
                        SELECT EXISTS (
                            SELECT 1 FROM webhook_deliveries WHERE id = :id AND merchant_id = :merchantId
                        )
                        """)
                .param("id", id)
                .param("merchantId", merchantId)
                .query(Boolean.class)
                .single();
    }

    public List<DeliveryRecord> listForMerchant(UUID merchantId) {
        return jdbcClient.sql("SELECT " + DELIVERY_COLUMNS + " FROM webhook_deliveries "
                        + "WHERE merchant_id = :merchantId ORDER BY created_at DESC, attempt DESC")
                .param("merchantId", merchantId)
                .query(WebhookDeliveryRepository::mapRow)
                .list();
    }

    public List<DeliveryRecord> listRecent() {
        return jdbcClient.sql("SELECT " + DELIVERY_COLUMNS + " FROM webhook_deliveries "
                        + "ORDER BY created_at DESC, attempt DESC LIMIT 200")
                .query(WebhookDeliveryRepository::mapRow)
                .list();
    }

    private static DeliveryRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp nextAttempt = rs.getTimestamp("next_attempt_at");
        return new DeliveryRecord(
                rs.getObject("id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getObject("merchant_id", UUID.class),
                rs.getInt("attempt"),
                rs.getString("status"),
                rs.getString("retry_topic"),
                nextAttempt == null ? null : nextAttempt.toInstant(),
                rs.getObject("last_status_code", Integer.class),
                rs.getString("last_error"),
                rs.getString("target_url"),
                rs.getString("payload_json"),
                rs.getString("traceparent"),
                rs.getTimestamp("created_at").toInstant());
    }

    public record DeliveryRecord(
            UUID id,
            UUID eventId,
            UUID merchantId,
            int attempt,
            String status,
            String retryTopic,
            Instant nextAttemptAt,
            Integer lastStatusCode,
            String lastError,
            String targetUrl,
            String payloadJson,
            String traceparent,
            Instant createdAt) {}
}

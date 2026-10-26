package com.paycore.webhook;

import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Owns durable dead-letter event persistence and replay-state updates. */
@Repository
public class WebhookDlqRepository {

    private static final String SELECT_COLUMNS =
            "id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at";

    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate;

    public WebhookDlqRepository(JdbcClient jdbcClient, PlatformTransactionManager transactionManager) {
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public UUID record(String eventId, String paymentId, String merchantId, byte[] rawPayload, String reason, Instant now) {
        UUID id = UuidV7.generate(now.toEpochMilli());
        jdbcClient.sql("""
                INSERT INTO dlq_events (id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at)
                VALUES (:id, :eventId, :paymentId, :merchantId, :rawPayload, :reason, FALSE, :createdAt)
                """)
                .param("id", id)
                .param("eventId", eventId)
                .param("paymentId", paymentId)
                .param("merchantId", merchantId)
                .param("rawPayload", rawPayload)
                .param("reason", reason)
                .param("createdAt", Timestamp.from(now))
                .update();
        return id;
    }

    /** Reads the row and marks it replayed in one transaction before callers perform network work. */
    public DlqEvent markReplayed(UUID id) {
        return transactionTemplate.execute(status -> {
            DlqEvent row = jdbcClient.sql("""
                            SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                            FROM dlq_events WHERE id = :id FOR UPDATE
                            """)
                    .param("id", id)
                    .query(WebhookDlqRepository::mapRow)
                    .optional()
                    .orElseThrow(() -> PayCoreException.notFound("DlqEvent", id));

            jdbcClient.sql("UPDATE dlq_events SET replayed = TRUE WHERE id = :id")
                    .param("id", id)
                    .update();

            return new DlqEvent(
                    row.id(), row.eventId(), row.paymentId(), row.merchantId(), row.rawPayload(),
                    row.reason(), true, row.createdAt());
        });
    }

    public List<DlqEvent> list() {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + " FROM dlq_events ORDER BY created_at DESC")
                .query(WebhookDlqRepository::mapRow)
                .list();
    }

    private static DlqEvent mapRow(java.sql.ResultSet rs, int rowNum)
            throws java.sql.SQLException {
        return new DlqEvent(
                rs.getObject("id", UUID.class),
                rs.getString("event_id"),
                rs.getString("payment_id"),
                rs.getString("merchant_id"),
                rs.getBytes("raw_payload"),
                rs.getString("reason"),
                rs.getBoolean("replayed"),
                rs.getTimestamp("created_at").toInstant());
    }

    public record DlqEvent(
            UUID id,
            String eventId,
            String paymentId,
            String merchantId,
            byte[] rawPayload,
            String reason,
            boolean replayed,
            Instant createdAt) {}
}

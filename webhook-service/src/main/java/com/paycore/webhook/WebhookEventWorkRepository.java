package com.paycore.webhook;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Owns durable claims and lease transitions for incoming webhook event work. */
@Repository
public class WebhookEventWorkRepository {

    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate;

    public WebhookEventWorkRepository(JdbcClient jdbcClient, PlatformTransactionManager transactionManager) {
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public byte[] claim(UUID eventId, byte[] payload, Instant now, int leaseSeconds) {
        return transactionTemplate.execute(status -> jdbcClient.sql("""
                        INSERT INTO webhook_event_work (event_id, raw_payload, status, lease_until, updated_at)
                        VALUES (:eventId, :payload, 'PROCESSING', :leaseUntil, :now)
                        ON CONFLICT (event_id) DO UPDATE
                        SET raw_payload = EXCLUDED.raw_payload, status = 'PROCESSING',
                            lease_until = EXCLUDED.lease_until, updated_at = EXCLUDED.updated_at
                        WHERE webhook_event_work.status = 'PROCESSING'
                          AND webhook_event_work.lease_until <= :now
                        RETURNING raw_payload
                        """)
                .param("eventId", eventId)
                .param("payload", payload)
                .param("leaseUntil", Timestamp.from(now.plusSeconds(leaseSeconds)))
                .param("now", Timestamp.from(now))
                .query((rs, rowNum) -> rs.getBytes("raw_payload"))
                .optional().orElse(null));
    }

    /** Claims a bounded batch atomically, so concurrent recovery workers cannot process the same rows. */
    public List<EventWorkClaim> claimExpired(Instant now, int leaseSeconds, int batchSize) {
        return transactionTemplate.execute(status -> jdbcClient.sql("""
                        WITH candidates AS (
                            SELECT event_id FROM webhook_event_work
                            WHERE status = 'PROCESSING' AND lease_until <= :now
                            ORDER BY lease_until, event_id
                            LIMIT :batchSize
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE webhook_event_work work
                        SET lease_until = :leaseUntil, updated_at = :now
                        FROM candidates
                        WHERE work.event_id = candidates.event_id
                          AND work.status = 'PROCESSING' AND work.lease_until <= :now
                        RETURNING work.event_id, work.raw_payload
                        """)
                .param("now", Timestamp.from(now))
                .param("leaseUntil", Timestamp.from(now.plusSeconds(leaseSeconds)))
                .param("batchSize", batchSize)
                .query((rs, rowNum) -> new EventWorkClaim(
                        rs.getObject("event_id", UUID.class), rs.getBytes("raw_payload")))
                .list());
    }

    public void complete(UUID eventId, Instant now) {
        jdbcClient.sql("""
                UPDATE webhook_event_work
                SET status = 'COMPLETE', lease_until = NULL, updated_at = :now
                WHERE event_id = :eventId AND status = 'PROCESSING'
                """)
                .param("eventId", eventId)
                .param("now", Timestamp.from(now))
                .update();
    }

    public void release(UUID eventId, Instant now) {
        jdbcClient.sql("""
                UPDATE webhook_event_work
                SET lease_until = :now, updated_at = :now
                WHERE event_id = :eventId AND status = 'PROCESSING'
                """)
                .param("eventId", eventId)
                .param("now", Timestamp.from(now))
                .update();
    }

    public record EventWorkClaim(UUID eventId, byte[] rawPayload) {}
}

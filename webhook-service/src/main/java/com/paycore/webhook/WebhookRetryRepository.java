package com.paycore.webhook;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Owns durable webhook retry claims and their lease-token guarded completion. */
@Repository
public class WebhookRetryRepository {

    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate;

    public WebhookRetryRepository(JdbcClient jdbcClient, PlatformTransactionManager transactionManager) {
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public List<RetryClaim> claimDue(Instant asOf, int leaseSeconds) {
        UUID claimToken = UUID.randomUUID();
        return transactionTemplate.execute(status -> jdbcClient.sql("""
                        WITH candidates AS (
                            SELECT id FROM webhook_deliveries
                            WHERE (status = 'RETRY_SCHEDULED' AND next_attempt_at <= :asOf)
                               OR (status = 'DELIVERING' AND lease_until <= :asOf)
                            ORDER BY COALESCE(next_attempt_at, lease_until), id
                            LIMIT 100
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE webhook_deliveries d
                        SET status = 'DELIVERING', claim_token = :claimToken,
                            lease_until = :leaseUntil
                        FROM candidates c
                        WHERE d.id = c.id
                        RETURNING d.id, d.event_id, d.merchant_id, d.attempt, d.target_url,
                                  d.payload_json, d.traceparent
                        """)
                .param("asOf", Timestamp.from(asOf))
                .param("claimToken", claimToken)
                .param("leaseUntil", Timestamp.from(asOf.plusSeconds(leaseSeconds)))
                .query((rs, rowNum) -> new RetryClaim(
                        rs.getObject("id", UUID.class),
                        rs.getObject("event_id", UUID.class),
                        rs.getObject("merchant_id", UUID.class),
                        rs.getInt("attempt"),
                        rs.getString("target_url"),
                        rs.getString("payload_json"),
                        rs.getString("traceparent"),
                        claimToken))
                .list());
    }

    public void finalizeClaim(UUID id, UUID claimToken, String status, String error) {
        jdbcClient.sql("""
                UPDATE webhook_deliveries
                SET status = :status, claim_token = NULL, lease_until = NULL,
                    last_error = COALESCE(:error, last_error)
                WHERE id = :id AND claim_token = :claimToken AND status = 'DELIVERING'
                """)
                .param("status", status)
                .param("error", error)
                .param("id", id)
                .param("claimToken", claimToken)
                .update();
    }

    public record RetryClaim(
            UUID id,
            UUID eventId,
            UUID merchantId,
            int attempt,
            String targetUrl,
            String payloadJson,
            String traceparent,
            UUID claimToken) {}
}

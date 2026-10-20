package com.paycore.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.client.RestTemplate;

/**
 * Webhook Delivery Engine with HMAC-SHA256 signing, SSRF protection, idempotent {@code processed_events}
 * deduplication, database-backed scheduled retries (1m, 5m, 30m, 2h, 6h) with jitter, and DLQ + replay.
 */
@Service
public class WebhookDeliveryService {

    public static final String CONSUMER_NAME = "webhook-service";
    public static final List<String> RETRY_TOPICS = List.of(
            "payments.events.retry-1m",
            "payments.events.retry-5m",
            "payments.events.retry-30m",
            "payments.events.retry-2h",
            "payments.events.retry-6h");
    public static final List<Duration> RETRY_DELAYS = List.of(
            Duration.ofMinutes(1),
            Duration.ofMinutes(5),
            Duration.ofMinutes(30),
            Duration.ofHours(2),
            Duration.ofHours(6));
    public static final String DLQ_TOPIC = "payments.events.dlq";

    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryService.class);

    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;
    private final ClockProvider clockProvider;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final WebhookHttpSender httpSender;
    private final boolean kafkaEnabled;
    private final TransactionTemplate transactionTemplate;
    private final int deliveryLeaseSeconds;
    private volatile boolean allowLocalInTest;

    public WebhookDeliveryService(
            JdbcClient jdbcClient,
            ObjectMapper objectMapper,
            ClockProvider clockProvider,
            @Autowired(required = false) KafkaTemplate<String, byte[]> kafkaTemplate,
            @Value("${paycore.webhook.kafka-listener-enabled:false}") boolean kafkaEnabled,
            @Value("${paycore.webhook.allow-local-targets:false}") boolean allowLocalInTest,
            @Value("${paycore.webhook.http-timeout-ms:2000}") int timeoutMs,
            PlatformTransactionManager transactionManager,
            @Value("${paycore.webhook.delivery-lease-seconds:30}") int deliveryLeaseSeconds) {
        this.jdbcClient = jdbcClient;
        this.objectMapper = objectMapper;
        this.clockProvider = clockProvider;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaEnabled = kafkaEnabled;
        this.allowLocalInTest = allowLocalInTest;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.deliveryLeaseSeconds = Math.max(1, deliveryLeaseSeconds);

        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(timeoutMs);
        factory.setReadTimeout(timeoutMs);
        this.httpSender = new WebhookHttpSender(new RestTemplate(factory));
    }

    public void registerMerchantEndpoint(UUID merchantId, String webhookUrl, String webhookSecret) {
        WebhookSsrfValidator.validateAtDeliveryTime(webhookUrl, allowLocalInTest);
        int updated = jdbcClient.sql("""
                UPDATE merchants
                SET webhook_url = :webhookUrl, webhook_secret = :webhookSecret
                WHERE id = :merchantId
                """)
                .param("merchantId", merchantId)
                .param("webhookUrl", webhookUrl)
                .param("webhookSecret", webhookSecret)
                .update();
        if (updated == 0) throw PayCoreException.notFound("Merchant", merchantId);
    }

    @KafkaListener(
            topics = "payments.events",
            groupId = "webhook-service-group",
            autoStartup = "${paycore.webhook.kafka-listener-enabled:false}")
    public void onKafkaPaymentEvent(byte[] rawPayload) {
        consumeRawEvent(rawPayload);
    }

    @Scheduled(fixedDelayString = "${paycore.webhook.retry-poll-interval-ms:1000}")
    public void scheduledRetryTick() {
        try {
            processDueRetries(clockProvider.now());
        } catch (Exception e) {
            log.warn("Webhook retry worker failed and will retry on the next poll: {}", e.getMessage());
        }
    }

    /**
     * Processes a raw Protobuf message from the event bus.
     * Poison messages are routed directly to the DLQ so they never block a partition (Section 6.2 #4 & 7.8).
     */
    public DeliveryOutcome consumeRawEvent(byte[] rawPayload) {
        PaymentEvent event;
        UUID eventId;
        try {
            event = PaymentEvent.parseFrom(rawPayload);
            if (event.getEventId().isBlank() || event.getMerchantId().isBlank()) {
                throw new InvalidProtocolBufferException("Missing required event_id or merchant_id in PaymentEvent");
            }
            eventId = UUID.fromString(event.getEventId());
            UUID.fromString(event.getMerchantId());
        } catch (Exception poisonEx) {
            UUID dlqId = recordDlqEvent(null, null, null, rawPayload, "POISON_MESSAGE: " + poisonEx.getMessage());
            log.warn("Routed poison message to DLQ (dlqId={}): {}", dlqId, poisonEx.getMessage());
            return new DeliveryOutcome(DeliveryStatus.DEAD_LETTERED, null, dlqId, 0);
        }

        byte[] claimedPayload = claimEventWork(eventId, rawPayload);
        if (claimedPayload == null) return new DeliveryOutcome(DeliveryStatus.DUPLICATE_IGNORED, null, null, 0);
        try {
            DeliveryOutcome outcome = processClaimedEvent(event);
            completeEventWork(eventId);
            return outcome;
        } catch (RuntimeException failure) {
            releaseEventWork(eventId);
            throw failure;
        }
    }

    private DeliveryOutcome processClaimedEvent(PaymentEvent event) {
        UUID eventId = UUID.fromString(event.getEventId());
        UUID merchantId = UUID.fromString(event.getMerchantId());
        int existingAttempts = countAttemptsForEvent(eventId);
        if (isAlreadyProcessed(eventId) || existingAttempts > 0) {
            return new DeliveryOutcome(DeliveryStatus.DUPLICATE_IGNORED, null, null, 0);
        }
        Optional<EndpointConfig> endpointOpt = findEndpoint(merchantId);
        if (endpointOpt.isEmpty()) {
            UUID dlqId = recordDlqEvent(eventId.toString(), event.getPaymentId(), merchantId.toString(),
                    event.toByteArray(), "MISSING_WEBHOOK_ENDPOINT");
            markProcessed(eventId);
            return new DeliveryOutcome(DeliveryStatus.DEAD_LETTERED, null, dlqId, 0);
        }
        EndpointConfig endpoint = endpointOpt.get();
        String traceparent = event.getTraceContextMap().getOrDefault(
                TraceHeaders.TRACEPARENT, TraceHeaders.currentTraceparentOrGenerate());
        return attemptDelivery(eventId, event.getPaymentId(), merchantId, existingAttempts + 1,
                endpoint.webhookUrl(), endpoint.webhookSecret(), buildWebhookJsonPayload(event),
                traceparent, event.toByteArray());
    }

    private byte[] claimEventWork(UUID eventId, byte[] payload) {
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
                .param("eventId", eventId).param("payload", payload)
                .param("leaseUntil", Timestamp.from(clockProvider.now().plusSeconds(deliveryLeaseSeconds)))
                .param("now", Timestamp.from(clockProvider.now()))
                .query((rs, rowNum) -> rs.getBytes("raw_payload"))
                .optional().orElse(null));
    }

    @Scheduled(fixedDelayString = "${paycore.webhook.event-work-poll-interval-ms:1000}")
    public void recoverExpiredEventWork() {
        try {
            recoverExpiredEventWorkBatch();
        } catch (Exception failure) {
            log.warn("Expired webhook event-work recovery failed and will retry on the next poll: {}",
                    failure.getMessage());
        }
    }

    private void recoverExpiredEventWorkBatch() {
        Instant now = clockProvider.now();
        List<UUID> expired = transactionTemplate.execute(status -> jdbcClient.sql("""
                        SELECT event_id FROM webhook_event_work
                        WHERE status = 'PROCESSING' AND lease_until <= :now
                        ORDER BY lease_until, event_id LIMIT 100 FOR UPDATE SKIP LOCKED
                        """)
                .param("now", Timestamp.from(now))
                .query(UUID.class).list());
        for (UUID eventId : expired) {
            byte[] payload = transactionTemplate.execute(status -> jdbcClient.sql("""
                            UPDATE webhook_event_work
                            SET lease_until = :leaseUntil, updated_at = :now
                            WHERE event_id = :eventId AND status = 'PROCESSING' AND lease_until <= :now
                            RETURNING raw_payload
                            """)
                    .param("eventId", eventId)
                    .param("now", Timestamp.from(now))
                    .param("leaseUntil", Timestamp.from(now.plusSeconds(deliveryLeaseSeconds)))
                    .query((rs, rowNum) -> rs.getBytes("raw_payload")).optional().orElse(null));
            if (payload == null) continue;
            try {
                PaymentEvent event = PaymentEvent.parseFrom(payload);
                processClaimedEvent(event);
                completeEventWork(eventId);
            } catch (Exception failure) {
                releaseEventWork(eventId);
                log.warn("Expired webhook event work {} failed and will be retried: {}", eventId, failure.getMessage());
            }
        }
    }

    private void completeEventWork(UUID eventId) {
        jdbcClient.sql("UPDATE webhook_event_work SET status = 'COMPLETE', lease_until = NULL, updated_at = :now WHERE event_id = :eventId")
                .param("eventId", eventId).param("now", Timestamp.from(clockProvider.now())).update();
    }

    private void releaseEventWork(UUID eventId) {
        jdbcClient.sql("UPDATE webhook_event_work SET lease_until = NOW(), updated_at = :now WHERE event_id = :eventId AND status = 'PROCESSING'")
                .param("eventId", eventId).param("now", Timestamp.from(clockProvider.now())).update();
    }

    /**
     * Executes all due scheduled retries from {@code webhook_deliveries} (delay-aware retry consumer).
     */
    public int processDueRetries(Instant asOf) {
        UUID claimToken = UUID.randomUUID();
        List<WebhookDeliveryRow> dueRows = transactionTemplate.execute(status -> jdbcClient.sql("""
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
                        RETURNING d.id, d.event_id, d.merchant_id, d.attempt, d.status, d.retry_topic,
                                  d.next_attempt_at, d.last_status_code, d.last_error, d.target_url,
                                  d.payload_json, d.traceparent, d.created_at
                        """)
                .param("asOf", Timestamp.from(asOf))
                .param("claimToken", claimToken)
                .param("leaseUntil", Timestamp.from(asOf.plusSeconds(deliveryLeaseSeconds)))
                .query(this::mapDeliveryRow)
                .list());

        int processed = 0;
        for (WebhookDeliveryRow row : dueRows) {
            if (isAlreadyProcessed(row.eventId())) {
                finalizeRetryClaim(row.id(), claimToken, "FAILED", null);
                continue;
            }
            boolean superseded = jdbcClient.sql("""
                            SELECT EXISTS (SELECT 1 FROM webhook_deliveries
                                           WHERE event_id = :eventId AND attempt > :attempt)
                            """)
                    .param("eventId", row.eventId())
                    .param("attempt", row.attempt())
                    .query(Boolean.class)
                    .single();
            if (superseded) {
                finalizeRetryClaim(row.id(), claimToken, "FAILED", "Superseded by a later delivery attempt");
                continue;
            }
            Optional<EndpointConfig> endpointOpt = findEndpoint(row.merchantId());
            if (endpointOpt.isEmpty()) {
                UUID dlqId = recordDlqEvent(row.eventId().toString(), null, row.merchantId().toString(),
                        row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8), "MISSING_WEBHOOK_ENDPOINT");
                markProcessed(row.eventId());
                finalizeRetryClaim(row.id(), claimToken, "FAILED", "MISSING_WEBHOOK_ENDPOINT");
                continue;
            }
            try {
                attemptDelivery(
                        row.eventId(), "", row.merchantId(), row.attempt() + 1,
                        row.targetUrl(), endpointOpt.get().webhookSecret(), row.payloadJson(), row.traceparent(),
                        row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
            } finally {
                finalizeRetryClaim(row.id(), claimToken, "FAILED", null);
            }
            processed++;
        }
        return processed;
    }

    private void finalizeRetryClaim(UUID id, UUID claimToken, String status, String error) {
        jdbcClient.sql("""
                UPDATE webhook_deliveries
                SET status = :status, claim_token = NULL, lease_until = NULL,
                    last_error = COALESCE(:error, last_error)
                WHERE id = :id AND claim_token = :claimToken AND status = 'DELIVERING'
                """)
                .param("status", status).param("error", error)
                .param("id", id).param("claimToken", claimToken).update();
    }

    public DeliveryOutcome replayDelivery(UUID deliveryId) {
        WebhookDeliveryRow row = jdbcClient.sql("""
                SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                       last_status_code, last_error, target_url, payload_json, traceparent, created_at
                FROM webhook_deliveries
                WHERE id = :id
                """)
                .param("id", deliveryId)
                .query(this::mapDeliveryRow)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("WebhookDelivery", deliveryId));

        EndpointConfig endpoint = findEndpoint(row.merchantId())
                .orElseThrow(() -> PayCoreException.notFound("MerchantWebhookEndpoint", row.merchantId()));

        int nextAttempt = countAttemptsForEvent(row.eventId()) + 1;
        return attemptDelivery(
                row.eventId(),
                "",
                row.merchantId(),
                nextAttempt,
                row.targetUrl(),
                endpoint.webhookSecret(),
                row.payloadJson(),
                row.traceparent(),
                row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public DeliveryOutcome replayDeliveryForMerchant(UUID merchantId, UUID deliveryId) {
        boolean owned = jdbcClient.sql("""
                SELECT EXISTS (
                    SELECT 1 FROM webhook_deliveries WHERE id = :id AND merchant_id = :merchantId
                )
                """)
                .param("id", deliveryId)
                .param("merchantId", merchantId)
                .query(Boolean.class)
                .single();
        if (!owned) {
            throw PayCoreException.notFound("WebhookDelivery", deliveryId);
        }
        return replayDelivery(deliveryId);
    }

    public DlqEventRow replayDlqEvent(UUID dlqId) {
        DlqEventRow dlq = jdbcClient.sql("""
                SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                FROM dlq_events
                WHERE id = :id
                FOR UPDATE
                """)
                .param("id", dlqId)
                .query(this::mapDlqRow)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("DlqEvent", dlqId));

        jdbcClient.sql("UPDATE dlq_events SET replayed = TRUE WHERE id = :id")
                .param("id", dlqId)
                .update();

        // If it's a valid PaymentEvent that exhausted retries, re-attempt delivery
        try {
            PaymentEvent event = PaymentEvent.parseFrom(dlq.rawPayload());
            if (!event.getEventId().isBlank() && !event.getMerchantId().isBlank()) {
                UUID eventId = UUID.fromString(event.getEventId());
                UUID merchantId = UUID.fromString(event.getMerchantId());
                Optional<EndpointConfig> ep = findEndpoint(merchantId);
                if (ep.isPresent()) {
                    String payloadJson = buildWebhookJsonPayload(event);
                    String traceparent = event.getTraceContextMap().getOrDefault(
                            TraceHeaders.TRACEPARENT, TraceHeaders.currentTraceparentOrGenerate());
                    attemptDelivery(
                            eventId,
                            event.getPaymentId(),
                            merchantId,
                            1, // Reset retry tier on explicit admin DLQ replay
                            ep.get().webhookUrl(),
                            ep.get().webhookSecret(),
                            payloadJson,
                            traceparent,
                            dlq.rawPayload());
                }
            }
        } catch (Exception ignored) {
            // Poison payload marked replayed/inspected
        }

        return jdbcClient.sql("""
                SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                FROM dlq_events WHERE id = :id
                """)
                .param("id", dlqId)
                .query(this::mapDlqRow)
                .single();
    }

    private DeliveryOutcome attemptDelivery(
            UUID eventId,
            String paymentId,
            UUID merchantId,
            int attempt,
            String targetUrl,
            String webhookSecret,
            String payloadJson,
            String traceparent,
            byte[] rawBytesForDlq) {

        Instant now = clockProvider.now();
        UUID deliveryId = UuidV7.generate(now.toEpochMilli());
        String timestampSec = String.valueOf(now.getEpochSecond());

        WebhookHttpSender.DeliveryResult httpResult = httpSender.send(
                targetUrl, webhookSecret, eventId, timestampSec, payloadJson, traceparent, allowLocalInTest);
        Integer statusCode = httpResult.statusCode();
        String errorMessage = httpResult.errorMessage();
        boolean delivered = httpResult.delivered();

        if (delivered) {
            insertDeliveryRow(
                    deliveryId,
                    eventId,
                    merchantId,
                    attempt,
                    "DELIVERED",
                    null,
                    null,
                    statusCode,
                    null,
                    targetUrl,
                    payloadJson,
                    traceparent,
                    now);
            markProcessed(eventId);
            return new DeliveryOutcome(DeliveryStatus.DELIVERED, deliveryId, null, attempt);
        }

        // Failed attempt: route to retry topic (attempts 1..5 -> retry-1m, 5m, 30m, 2h, 6h) or DLQ on attempt > 5
        if (attempt <= RETRY_TOPICS.size()) {
            String retryTopic = RETRY_TOPICS.get(attempt - 1);
            Duration baseDelay = RETRY_DELAYS.get(attempt - 1);
            long jitterMs = ThreadLocalRandom.current().nextLong(0, 1000);
            Instant nextAttemptAt = now.plus(baseDelay).plusMillis(jitterMs);

            insertDeliveryRow(
                    deliveryId,
                    eventId,
                    merchantId,
                    attempt,
                    "RETRY_SCHEDULED",
                    retryTopic,
                    nextAttemptAt,
                    statusCode,
                    errorMessage,
                    targetUrl,
                    payloadJson,
                    traceparent,
                    now);

            return new DeliveryOutcome(DeliveryStatus.RETRY_SCHEDULED, deliveryId, null, attempt);
        } else {
            insertDeliveryRow(
                    deliveryId,
                    eventId,
                    merchantId,
                    attempt,
                    "DEAD_LETTERED",
                    DLQ_TOPIC,
                    null,
                    statusCode,
                    errorMessage,
                    targetUrl,
                    payloadJson,
                    traceparent,
                    now);
            UUID dlqId = recordDlqEvent(
                    eventId.toString(),
                    paymentId,
                    merchantId.toString(),
                    rawBytesForDlq,
                    "MAX_RETRIES_EXCEEDED: " + errorMessage);
            return new DeliveryOutcome(DeliveryStatus.DEAD_LETTERED, deliveryId, dlqId, attempt);
        }
    }

    private void insertDeliveryRow(
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
            Instant createdAt) {
        jdbcClient.sql("""
                INSERT INTO webhook_deliveries (
                    id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                    last_status_code, last_error, target_url, payload_json, traceparent, created_at
                ) VALUES (
                    :id, :eventId, :merchantId, :attempt, :status, :retryTopic, :nextAttemptAt,
                    :lastStatusCode, :lastError, :targetUrl, :payloadJson, :traceparent, :createdAt
                )
                """)
                .param("id", id)
                .param("eventId", eventId)
                .param("merchantId", merchantId)
                .param("attempt", attempt)
                .param("status", status)
                .param("retryTopic", retryTopic)
                .param("nextAttemptAt", nextAttemptAt != null ? Timestamp.from(nextAttemptAt) : null)
                .param("lastStatusCode", lastStatusCode)
                .param("lastError", lastError)
                .param("targetUrl", targetUrl)
                .param("payloadJson", payloadJson)
                .param("traceparent", traceparent)
                .param("createdAt", Timestamp.from(createdAt))
                .update();
    }

    private UUID recordDlqEvent(
            String eventId, String paymentId, String merchantId, byte[] rawPayload, String reason) {
        UUID dlqId = UuidV7.generate(clockProvider.now().toEpochMilli());
        jdbcClient.sql("""
                INSERT INTO dlq_events (id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at)
                VALUES (:id, :eventId, :paymentId, :merchantId, :rawPayload, :reason, FALSE, :createdAt)
                """)
                .param("id", dlqId)
                .param("eventId", eventId)
                .param("paymentId", paymentId)
                .param("merchantId", merchantId)
                .param("rawPayload", rawPayload)
                .param("reason", reason)
                .param("createdAt", Timestamp.from(clockProvider.now()))
                .update();
        return dlqId;
    }

    private void publishToKafkaTopicSafely(String topic, String key, byte[] payload) {
        if (!kafkaEnabled || kafkaTemplate == null) {
            return;
        }
        try {
            kafkaTemplate.send(topic, key != null ? key : "", payload);
        } catch (Exception e) {
            log.debug("Kafka publish to {} skipped: {}", topic, e.getMessage());
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

    private int countAttemptsForEvent(UUID eventId) {
        Integer cnt = jdbcClient.sql("SELECT COUNT(*) FROM webhook_deliveries WHERE event_id = :eventId")
                .param("eventId", eventId)
                .query(Integer.class)
                .single();
        return cnt != null ? cnt : 0;
    }

    private Optional<EndpointConfig> findEndpoint(UUID merchantId) {
        return jdbcClient.sql("""
                SELECT webhook_url, webhook_secret
                FROM merchants
                WHERE id = :merchantId AND webhook_url IS NOT NULL AND webhook_secret IS NOT NULL
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new EndpointConfig(
                        rs.getString("webhook_url"), rs.getString("webhook_secret")))
                .optional();
    }

    private String buildWebhookJsonPayload(PaymentEvent event) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("event_id", event.getEventId());
        root.put("event_type", event.getEventType());
        root.put("schema_version", event.getSchemaVersion());
        root.put("payment_id", event.getPaymentId());
        root.put("merchant_id", event.getMerchantId());
        root.put("event_seq", event.getEventSeq());
        root.put("occurred_at", Instant.ofEpochSecond(
                event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos()).toString());

        Map<String, Object> data = new LinkedHashMap<>();
        switch (event.getPayloadCase()) {
            case AUTHORIZED -> {
                data.put("amount", event.getAuthorized().getAmount());
                data.put("currency", event.getAuthorized().getCurrency());
                data.put("psp_ref", event.getAuthorized().getPspRef());
            }
            case CAPTURED -> {
                data.put("captured_amount", event.getCaptured().getCapturedAmount());
                data.put("fee_amount", event.getCaptured().getFeeAmount());
                data.put("net_merchant_amount", event.getCaptured().getNetMerchantAmount());
                data.put("currency", event.getCaptured().getCurrency());
                data.put("psp_ref", event.getCaptured().getPspRef());
                data.put("ledger_txn_id", event.getCaptured().getLedgerTxnId());
            }
            case REFUNDED -> {
                data.put("refund_id", event.getRefunded().getRefundId());
                data.put("refund_amount", event.getRefunded().getRefundAmount());
                data.put("fee_reversed_amount", event.getRefunded().getFeeReversedAmount());
                data.put("total_refunded_amount", event.getRefunded().getTotalRefundedAmount());
                data.put("currency", event.getRefunded().getCurrency());
                data.put("ledger_txn_id", event.getRefunded().getLedgerTxnId());
            }
            case PAYOUT -> {
                data.put("payout_id", event.getPayout().getPayoutId());
                data.put("amount", event.getPayout().getAmount());
                data.put("currency", event.getPayout().getCurrency());
                data.put("ledger_txn_id", event.getPayout().getLedgerTxnId());
            }
            case FAILED -> {
                data.put("amount", event.getFailed().getAmount());
                data.put("currency", event.getFailed().getCurrency());
                data.put("psp_ref", event.getFailed().getPspRef());
                data.put("reason", event.getFailed().getReason());
            }
            default -> {}
        }
        root.put("data", data);

        try {
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize webhook JSON", e);
        }
    }

    @Transactional(readOnly = true)
    public List<WebhookDeliveryRow> listDeliveries(UUID merchantId) {
        if (merchantId != null) {
            return jdbcClient.sql("""
                    SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                           last_status_code, last_error, target_url, payload_json, traceparent, created_at
                    FROM webhook_deliveries
                    WHERE merchant_id = :merchantId
                    ORDER BY created_at DESC, attempt DESC
                    """)
                    .param("merchantId", merchantId)
                    .query(this::mapDeliveryRow)
                    .list();
        }
        return jdbcClient.sql("""
                SELECT id, event_id, merchant_id, attempt, status, retry_topic, next_attempt_at,
                       last_status_code, last_error, target_url, payload_json, traceparent, created_at
                FROM webhook_deliveries
                ORDER BY created_at DESC, attempt DESC
                LIMIT 200
                """)
                .query(this::mapDeliveryRow)
                .list();
    }

    @Transactional(readOnly = true)
    public List<DlqEventRow> listDlqEvents() {
        return jdbcClient.sql("""
                SELECT id, event_id, payment_id, merchant_id, raw_payload, reason, replayed, created_at
                FROM dlq_events
                ORDER BY created_at DESC
                """)
                .query(this::mapDlqRow)
                .list();
    }

    @Transactional(readOnly = true)
    public ConsumerLagView getConsumerLagStats() {
        long pendingRetries = jdbcClient.sql(
                        "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'RETRY_SCHEDULED'")
                .query(Long.class)
                .single();
        long dlqDepth = jdbcClient.sql(
                        "SELECT COUNT(*) FROM dlq_events WHERE replayed = FALSE")
                .query(Long.class)
                .single();
        long processedTotal = jdbcClient.sql(
                        "SELECT COUNT(*) FROM processed_events WHERE consumer = :c")
                .param("c", CONSUMER_NAME)
                .query(Long.class)
                .single();
        return new ConsumerLagView(CONSUMER_NAME, pendingRetries, dlqDepth, processedTotal, clockProvider.now());
    }

    public void setAllowLocalInTest(boolean allowLocalInTest) {
        this.allowLocalInTest = allowLocalInTest;
    }

    private WebhookDeliveryRow mapDeliveryRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp nextTs = rs.getTimestamp("next_attempt_at");
        return new WebhookDeliveryRow(
                rs.getObject("id", UUID.class),
                rs.getObject("event_id", UUID.class),
                rs.getObject("merchant_id", UUID.class),
                rs.getInt("attempt"),
                rs.getString("status"),
                rs.getString("retry_topic"),
                nextTs != null ? nextTs.toInstant() : null,
                rs.getObject("last_status_code", Integer.class),
                rs.getString("last_error"),
                rs.getString("target_url"),
                rs.getString("payload_json"),
                rs.getString("traceparent"),
                rs.getTimestamp("created_at").toInstant());
    }

    private DlqEventRow mapDlqRow(ResultSet rs, int rowNum) throws SQLException {
        return new DlqEventRow(
                rs.getObject("id", UUID.class),
                rs.getString("event_id"),
                rs.getString("payment_id"),
                rs.getString("merchant_id"),
                rs.getBytes("raw_payload"),
                rs.getString("reason"),
                rs.getBoolean("replayed"),
                rs.getTimestamp("created_at").toInstant());
    }

    private record EndpointConfig(String webhookUrl, String webhookSecret) {}

    public enum DeliveryStatus {
        DELIVERED,
        DUPLICATE_IGNORED,
        RETRY_SCHEDULED,
        DEAD_LETTERED
    }

    public record DeliveryOutcome(
            DeliveryStatus status, UUID deliveryId, UUID dlqId, int attempt) {}

    public record WebhookDeliveryRow(
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

    public record DlqEventRow(
            UUID id,
            String eventId,
            String paymentId,
            String merchantId,
            byte[] rawPayload,
            String reason,
            boolean replayed,
            Instant createdAt) {}

    public record ConsumerLagView(
            String consumer,
            long pendingRetries,
            long dlqDepth,
            long processedEventsTotal,
            Instant checkedAt) {}
}

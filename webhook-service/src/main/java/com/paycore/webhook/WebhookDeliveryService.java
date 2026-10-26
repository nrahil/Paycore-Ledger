package com.paycore.webhook;

import com.google.protobuf.InvalidProtocolBufferException;
import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.tracing.TraceHeaders;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
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
    private final ClockProvider clockProvider;
    private final WebhookHttpSender httpSender;
    private final WebhookPayloadMapper payloadMapper;
    private final WebhookEventWorkRepository eventWorkRepository;
    private final WebhookDlqRepository dlqRepository;
    private final WebhookDeliveryRepository deliveryRepository;
    private final WebhookDeliveryOutcomeService outcomeService;
    private final int deliveryLeaseSeconds;
    private volatile boolean allowLocalInTest;

    public WebhookDeliveryService(
            JdbcClient jdbcClient,
            ClockProvider clockProvider,
            @Value("${paycore.webhook.allow-local-targets:false}") boolean allowLocalInTest,
            @Value("${paycore.webhook.http-timeout-ms:2000}") int timeoutMs,
            @Value("${paycore.webhook.delivery-lease-seconds:30}") int deliveryLeaseSeconds,
            WebhookPayloadMapper payloadMapper,
            WebhookEventWorkRepository eventWorkRepository,
            WebhookDlqRepository dlqRepository,
            WebhookDeliveryRepository deliveryRepository,
            WebhookDeliveryOutcomeService outcomeService) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.payloadMapper = payloadMapper;
        this.eventWorkRepository = eventWorkRepository;
        this.dlqRepository = dlqRepository;
        this.deliveryRepository = deliveryRepository;
        this.outcomeService = outcomeService;
        this.allowLocalInTest = allowLocalInTest;
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
        int existingAttempts = deliveryRepository.countAttempts(eventId);
        if (deliveryRepository.isProcessed(eventId, CONSUMER_NAME) || existingAttempts > 0) {
            return new DeliveryOutcome(DeliveryStatus.DUPLICATE_IGNORED, null, null, 0);
        }
        Optional<EndpointConfig> endpointOpt = findEndpoint(merchantId);
        if (endpointOpt.isEmpty()) {
            UUID dlqId = outcomeService.recordMissingEndpoint(eventId, event.getPaymentId(), merchantId,
                    event.toByteArray(), "MISSING_WEBHOOK_ENDPOINT", CONSUMER_NAME, clockProvider.now());
            return new DeliveryOutcome(DeliveryStatus.DEAD_LETTERED, null, dlqId, 0);
        }
        EndpointConfig endpoint = endpointOpt.get();
        String traceparent = event.getTraceContextMap().getOrDefault(
                TraceHeaders.TRACEPARENT, TraceHeaders.currentTraceparentOrGenerate());
        return attemptDelivery(eventId, event.getPaymentId(), merchantId, existingAttempts + 1,
                endpoint.webhookUrl(), endpoint.webhookSecret(), payloadMapper.toJson(event),
                traceparent, event.toByteArray());
    }

    private byte[] claimEventWork(UUID eventId, byte[] payload) {
        return eventWorkRepository.claim(eventId, payload, clockProvider.now(), deliveryLeaseSeconds);
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
        List<WebhookEventWorkRepository.EventWorkClaim> expired = eventWorkRepository.claimExpired(
                now, deliveryLeaseSeconds, 100);
        for (WebhookEventWorkRepository.EventWorkClaim claim : expired) {
            UUID eventId = claim.eventId();
            try {
                PaymentEvent event = PaymentEvent.parseFrom(claim.rawPayload());
                processClaimedEvent(event);
                completeEventWork(eventId);
            } catch (Exception failure) {
                releaseEventWork(eventId);
                log.warn("Expired webhook event work {} failed and will be retried: {}", eventId, failure.getMessage());
            }
        }
    }

    private void completeEventWork(UUID eventId) {
        eventWorkRepository.complete(eventId, clockProvider.now());
    }

    private void releaseEventWork(UUID eventId) {
        eventWorkRepository.release(eventId, clockProvider.now());
    }

    RetryProcessingResult processRetryClaim(WebhookRetryRepository.RetryClaim row) {
        if (deliveryRepository.isProcessed(row.eventId(), CONSUMER_NAME)) {
            return new RetryProcessingResult(false, null);
        }
        if (deliveryRepository.hasLaterAttempt(row.eventId(), row.attempt())) {
            return new RetryProcessingResult(false, "Superseded by a later delivery attempt");
        }
        Optional<EndpointConfig> endpointOpt = findEndpoint(row.merchantId());
        if (endpointOpt.isEmpty()) {
            outcomeService.recordMissingEndpoint(row.eventId(), null, row.merchantId(),
                    row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    "MISSING_WEBHOOK_ENDPOINT", CONSUMER_NAME, clockProvider.now());
            return new RetryProcessingResult(false, "MISSING_WEBHOOK_ENDPOINT");
        }
        attemptDelivery(
                row.eventId(), "", row.merchantId(), row.attempt() + 1,
                row.targetUrl(), endpointOpt.get().webhookSecret(), row.payloadJson(), row.traceparent(),
                row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return new RetryProcessingResult(true, null);
    }

    record RetryProcessingResult(boolean attempted, String error) {}

    DeliveryOutcome attemptDelivery(
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
            outcomeService.recordDelivered(deliveryRecord(
                    deliveryId, eventId, merchantId, attempt, "DELIVERED", null, null,
                    statusCode, null, targetUrl, payloadJson, traceparent, now),
                    CONSUMER_NAME, clockProvider.now());
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
            UUID dlqId = outcomeService.recordDeadLetter(deliveryRecord(
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
                    now),
                    paymentId,
                    rawBytesForDlq,
                    "MAX_RETRIES_EXCEEDED: " + errorMessage,
                    now);
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
        deliveryRepository.insert(deliveryRecord(id, eventId, merchantId, attempt, status, retryTopic,
                nextAttemptAt, lastStatusCode, lastError, targetUrl, payloadJson, traceparent, createdAt));
    }

    private WebhookDeliveryRepository.DeliveryRecord deliveryRecord(
            UUID id, UUID eventId, UUID merchantId, int attempt, String status, String retryTopic,
            Instant nextAttemptAt, Integer lastStatusCode, String lastError, String targetUrl,
            String payloadJson, String traceparent, Instant createdAt) {
        return new WebhookDeliveryRepository.DeliveryRecord(id, eventId, merchantId, attempt, status,
                retryTopic, nextAttemptAt, lastStatusCode, lastError, targetUrl, payloadJson, traceparent, createdAt);
    }

    private UUID recordDlqEvent(
            String eventId, String paymentId, String merchantId, byte[] rawPayload, String reason) {
        return dlqRepository.record(eventId, paymentId, merchantId, rawPayload, reason, clockProvider.now());
    }

    Optional<EndpointConfig> findEndpoint(UUID merchantId) {
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

    public void setAllowLocalInTest(boolean allowLocalInTest) {
        this.allowLocalInTest = allowLocalInTest;
    }

    record EndpointConfig(String webhookUrl, String webhookSecret) {}

    public enum DeliveryStatus {
        DELIVERED,
        DUPLICATE_IGNORED,
        RETRY_SCHEDULED,
        DEAD_LETTERED
    }

    public record DeliveryOutcome(
            DeliveryStatus status, UUID deliveryId, UUID dlqId, int attempt) {}

}

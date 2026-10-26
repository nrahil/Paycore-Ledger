package com.paycore.webhook;

import java.time.Instant;
import java.util.UUID;

/** Response views returned by the webhook query and replay APIs. */
public final class WebhookViews {

    private WebhookViews() {}

    public record DeliveryRow(
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

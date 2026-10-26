package com.paycore.webhook;

import com.paycore.common.errors.PayCoreException;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.tracing.TraceHeaders;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;

/** Coordinates merchant and operator initiated replay requests. */
@Service
public class WebhookReplayService {

    private final WebhookDeliveryService deliveryService;
    private final WebhookDeliveryRepository deliveryRepository;
    private final WebhookDlqRepository dlqRepository;
    private final WebhookPayloadMapper payloadMapper;

    public WebhookReplayService(
            WebhookDeliveryService deliveryService,
            WebhookDeliveryRepository deliveryRepository,
            WebhookDlqRepository dlqRepository,
            WebhookPayloadMapper payloadMapper) {
        this.deliveryService = deliveryService;
        this.deliveryRepository = deliveryRepository;
        this.dlqRepository = dlqRepository;
        this.payloadMapper = payloadMapper;
    }

    public WebhookDeliveryService.DeliveryOutcome replayDelivery(UUID deliveryId) {
        WebhookDeliveryRepository.DeliveryRecord row = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> PayCoreException.notFound("WebhookDelivery", deliveryId));

        WebhookDeliveryService.EndpointConfig endpoint = deliveryService.findEndpoint(row.merchantId())
                .orElseThrow(() -> PayCoreException.notFound("MerchantWebhookEndpoint", row.merchantId()));

        int nextAttempt = deliveryRepository.countAttempts(row.eventId()) + 1;
        return deliveryService.attemptDelivery(
                row.eventId(), "", row.merchantId(), nextAttempt,
                row.targetUrl(), endpoint.webhookSecret(), row.payloadJson(), row.traceparent(),
                row.payloadJson().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public WebhookDeliveryService.DeliveryOutcome replayDeliveryForMerchant(UUID merchantId, UUID deliveryId) {
        if (!deliveryRepository.existsForMerchant(deliveryId, merchantId)) {
            throw PayCoreException.notFound("WebhookDelivery", deliveryId);
        }
        return replayDelivery(deliveryId);
    }

    public WebhookViews.DlqEventRow replayDlqEvent(UUID dlqId) {
        WebhookDlqRepository.DlqEvent dlq = dlqRepository.markReplayed(dlqId);

        // If it's a valid PaymentEvent that exhausted retries, re-attempt delivery.
        try {
            PaymentEvent event = PaymentEvent.parseFrom(dlq.rawPayload());
            if (!event.getEventId().isBlank() && !event.getMerchantId().isBlank()) {
                UUID eventId = UUID.fromString(event.getEventId());
                UUID merchantId = UUID.fromString(event.getMerchantId());
                Optional<WebhookDeliveryService.EndpointConfig> endpoint = deliveryService.findEndpoint(merchantId);
                if (endpoint.isPresent()) {
                    String payloadJson = payloadMapper.toJson(event);
                    String traceparent = event.getTraceContextMap().getOrDefault(
                            TraceHeaders.TRACEPARENT, TraceHeaders.currentTraceparentOrGenerate());
                    deliveryService.attemptDelivery(
                            eventId,
                            event.getPaymentId(),
                            merchantId,
                            1, // Reset retry tier on explicit admin DLQ replay
                            endpoint.get().webhookUrl(),
                            endpoint.get().webhookSecret(),
                            payloadJson,
                            traceparent,
                            dlq.rawPayload());
                }
            }
        } catch (Exception ignored) {
            // Poison payload marked replayed/inspected
        }

        return WebhookQueryService.toDlqView(dlq);
    }
}

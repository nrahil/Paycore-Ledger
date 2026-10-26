package com.paycore.webhook;

import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Commits related webhook outcome records together after the external HTTP attempt has finished. */
@Service
public class WebhookDeliveryOutcomeService {

    private final WebhookDeliveryRepository deliveryRepository;
    private final WebhookDlqRepository dlqRepository;
    private final TransactionTemplate transactionTemplate;

    public WebhookDeliveryOutcomeService(
            WebhookDeliveryRepository deliveryRepository,
            WebhookDlqRepository dlqRepository,
            PlatformTransactionManager transactionManager) {
        this.deliveryRepository = deliveryRepository;
        this.dlqRepository = dlqRepository;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    public void recordDelivered(
            WebhookDeliveryRepository.DeliveryRecord delivery, String consumer, Instant processedAt) {
        transactionTemplate.executeWithoutResult(status -> {
            deliveryRepository.insert(delivery);
            deliveryRepository.markProcessed(delivery.eventId(), consumer, processedAt);
        });
    }

    public UUID recordMissingEndpoint(
            UUID eventId,
            String paymentId,
            UUID merchantId,
            byte[] rawPayload,
            String reason,
            String consumer,
            Instant recordedAt) {
        return transactionTemplate.execute(status -> {
            UUID dlqId = dlqRepository.record(
                    eventId.toString(), paymentId, merchantId.toString(), rawPayload, reason, recordedAt);
            deliveryRepository.markProcessed(eventId, consumer, recordedAt);
            return dlqId;
        });
    }

    public UUID recordDeadLetter(
            WebhookDeliveryRepository.DeliveryRecord delivery,
            String paymentId,
            byte[] rawPayload,
            String reason,
            Instant recordedAt) {
        return transactionTemplate.execute(status -> {
            deliveryRepository.insert(delivery);
            return dlqRepository.record(
                    delivery.eventId().toString(), paymentId, delivery.merchantId().toString(),
                    rawPayload, reason, recordedAt);
        });
    }
}

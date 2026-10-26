package com.paycore.webhook;

import com.paycore.common.clock.ClockProvider;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Serves read-only webhook delivery, DLQ, and consumer-lag views. */
@Service
public class WebhookQueryService {

    private final WebhookDeliveryRepository deliveryRepository;
    private final WebhookDlqRepository dlqRepository;
    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;

    public WebhookQueryService(
            WebhookDeliveryRepository deliveryRepository,
            WebhookDlqRepository dlqRepository,
            JdbcClient jdbcClient,
            ClockProvider clockProvider) {
        this.deliveryRepository = deliveryRepository;
        this.dlqRepository = dlqRepository;
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
    }

    @Transactional(readOnly = true)
    public List<WebhookViews.DeliveryRow> listDeliveries(UUID merchantId) {
        List<WebhookDeliveryRepository.DeliveryRecord> rows = merchantId == null
                ? deliveryRepository.listRecent()
                : deliveryRepository.listForMerchant(merchantId);
        return rows.stream().map(WebhookQueryService::toDeliveryView).toList();
    }

    @Transactional(readOnly = true)
    public List<WebhookViews.DlqEventRow> listDlqEvents() {
        return dlqRepository.list().stream().map(WebhookQueryService::toDlqView).toList();
    }

    @Transactional(readOnly = true)
    public WebhookViews.ConsumerLagView getConsumerLagStats() {
        long pendingRetries = jdbcClient.sql(
                        "SELECT COUNT(*) FROM webhook_deliveries WHERE status = 'RETRY_SCHEDULED'")
                .query(Long.class)
                .single();
        long dlqDepth = jdbcClient.sql("SELECT COUNT(*) FROM dlq_events WHERE replayed = FALSE")
                .query(Long.class)
                .single();
        long processedTotal = jdbcClient.sql("SELECT COUNT(*) FROM processed_events WHERE consumer = :consumer")
                .param("consumer", WebhookDeliveryService.CONSUMER_NAME)
                .query(Long.class)
                .single();
        return new WebhookViews.ConsumerLagView(
                WebhookDeliveryService.CONSUMER_NAME, pendingRetries, dlqDepth, processedTotal, clockProvider.now());
    }

    static WebhookViews.DeliveryRow toDeliveryView(WebhookDeliveryRepository.DeliveryRecord row) {
        return new WebhookViews.DeliveryRow(row.id(), row.eventId(), row.merchantId(), row.attempt(), row.status(),
                row.retryTopic(), row.nextAttemptAt(), row.lastStatusCode(), row.lastError(), row.targetUrl(),
                row.payloadJson(), row.traceparent(), row.createdAt());
    }

    static WebhookViews.DlqEventRow toDlqView(WebhookDlqRepository.DlqEvent row) {
        return new WebhookViews.DlqEventRow(row.id(), row.eventId(), row.paymentId(), row.merchantId(),
                row.rawPayload(), row.reason(), row.replayed(), row.createdAt());
    }
}

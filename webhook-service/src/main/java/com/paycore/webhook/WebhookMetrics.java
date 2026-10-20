package com.paycore.webhook;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Exposes durable webhook backlog state without making metrics requests perform database work. */
@Component
public class WebhookMetrics {

    private final JdbcClient jdbcClient;
    private final AtomicLong dlqDepth = new AtomicLong();

    public WebhookMetrics(MeterRegistry registry, JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
        Gauge.builder("paycore_webhook_dlq_depth", dlqDepth, AtomicLong::get)
                .description("Number of webhook dead-letter records awaiting replay")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${paycore.webhook.metrics-refresh-ms:15000}")
    public void refreshDlqDepth() {
        Long count = jdbcClient.sql("SELECT COUNT(*) FROM dlq_events WHERE replayed = FALSE")
                .query(Long.class)
                .single();
        dlqDepth.set(count);
    }
}

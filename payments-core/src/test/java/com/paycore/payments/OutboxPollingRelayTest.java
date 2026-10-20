package com.paycore.payments;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import com.paycore.payments.outbox.OutboxPollingRelay;
import com.paycore.payments.outbox.OutboxWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies Outbox Polling Relay v1 (Section 7.7):
 * - leased claiming, per-aggregate sequence ordering and crash recovery
 * - Mid-batch broker crash simulation (at-least-once delivery + downstream event_id deduplication)
 */
@SpringBootTest
@Testcontainers
class OutboxPollingRelayTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_outbox_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("paycore.redis.enabled", () -> "false");
        registry.add("paycore.outbox.relay-enabled", () -> "false");
        registry.add("paycore.resolver.interval-ms", () -> "3600000");
    }

    @Autowired
    private OutboxWriter outboxWriter;

    @Autowired
    private OutboxPollingRelay outboxPollingRelay;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TransactionTemplate txTemplate;

    @BeforeEach
    void setUp() {
        jdbcClient.sql("TRUNCATE TABLE outbox_events").update();
        outboxPollingRelay.setCrashMidBatchHook(null);
        outboxPollingRelay.setCustomPublishSink(null);
    }

    @Test
    @DisplayName("Outbox relay preserves aggregate order and recovers an expired claim after publish-before-ack crash")
    void midBatchCrashRecoversWithZeroLostEvents() throws Exception {
        UUID merchantId = UuidV7.generate();
        UUID paymentId = UuidV7.generate();

        // Write 5 events into outbox_events
        for (int i = 1; i <= 5; i++) {
            final long seq = i;
            final long amount = i * 1_000L;
            txTemplate.executeWithoutResult(status -> outboxWriter.writePaymentAuthorized(
                    paymentId, merchantId, seq, amount, "INR", "psp_" + amount));
        }

        assertThat(countUnpublished()).isEqualTo(5L);

        List<byte[]> brokerReceivedRaw = new ArrayList<>();
        AtomicBoolean failAfterFirstPublish = new AtomicBoolean(true);

        outboxPollingRelay.setCustomPublishSink(row -> brokerReceivedRaw.add(row.payload()));
        outboxPollingRelay.setCrashMidBatchHook(index -> {
            if (failAfterFirstPublish.get() && index == 0) {
                throw new RuntimeException("Simulated crash after broker publish and before database acknowledgement");
            }
        });

        // First call publishes seq 1, then simulates a crash before the database acknowledgement.
        assertThatThrownBy(() -> outboxPollingRelay.relayNextBatch())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("before database acknowledgement");

        // All 5 events remain unpublished in outbox_events so nothing is ever lost
        assertThat(countUnpublished()).isEqualTo(5L);

        // Lease prevents duplicate immediate claims; after lease expiry the event is safely retried.
        assertThat(outboxPollingRelay.relayNextBatch()).isZero();
        jdbcClient.sql("UPDATE outbox_events SET lease_until = NOW() - INTERVAL '1 second'").update();
        failAfterFirstPublish.set(false);
        int publishedCount = 0;
        for (int i = 0; i < 5; i++) publishedCount += outboxPollingRelay.relayNextBatch();
        assertThat(publishedCount).isEqualTo(5);
        assertThat(countUnpublished()).isEqualTo(0L);

        // The first publish is repeated after recovery; sequence ordering is preserved across polls.
        assertThat(brokerReceivedRaw).hasSize(6);

        // Downstream consumer deduplicating by event_id sees exactly the 5 unique events!
        Set<String> deduplicatedEventIds = new LinkedHashSet<>();
        for (byte[] raw : brokerReceivedRaw) {
            PaymentEvent ev = PaymentEvent.parseFrom(raw);
            deduplicatedEventIds.add(ev.getEventId());
        }
        assertThat(deduplicatedEventIds).hasSize(5);
        List<Long> observedSequences = brokerReceivedRaw.stream()
                .map(bytes -> {
                    try { return PaymentEvent.parseFrom(bytes).getEventSeq(); }
                    catch (Exception e) { throw new AssertionError(e); }
                }).toList();
        assertThat(observedSequences).containsExactly(1L, 1L, 2L, 3L, 4L, 5L);
    }

    @Test
    @DisplayName("Broker errors release the claim, persist backoff metadata and allow retry")
    void brokerErrorSchedulesRetryWithoutLosingEvent() {
        UUID merchantId = UuidV7.generate();
        UUID paymentId = UuidV7.generate();
        txTemplate.executeWithoutResult(status -> outboxWriter.writePaymentAuthorized(
                paymentId, merchantId, 1L, 1_000L, "INR", "psp_retry"));

        outboxPollingRelay.setCustomPublishSink(row -> { throw new RuntimeException("broker unavailable"); });
        assertThatThrownBy(() -> outboxPollingRelay.relayNextBatch())
                .isInstanceOf(RuntimeException.class).hasMessageContaining("broker unavailable");
        assertThat(countUnpublished()).isEqualTo(1L);
        assertThat(jdbcClient.sql("SELECT attempt_count FROM outbox_events WHERE aggregate_id = :id")
                .param("id", paymentId).query(Integer.class).single()).isEqualTo(1);
        assertThat(jdbcClient.sql("SELECT last_error FROM outbox_events WHERE aggregate_id = :id")
                .param("id", paymentId).query(String.class).single()).contains("broker unavailable");

        jdbcClient.sql("UPDATE outbox_events SET next_attempt_at = NOW() WHERE aggregate_id = :id")
                .param("id", paymentId).update();
        outboxPollingRelay.setCustomPublishSink(row -> {});
        assertThat(outboxPollingRelay.relayNextBatch()).isEqualTo(1);
        assertThat(countUnpublished()).isZero();
    }

    private long countUnpublished() {
        Long cnt = jdbcClient.sql("SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL")
                .query(Long.class)
                .single();
        return cnt != null ? cnt : 0L;
    }
}

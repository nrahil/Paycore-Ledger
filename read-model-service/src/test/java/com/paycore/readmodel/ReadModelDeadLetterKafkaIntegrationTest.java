package com.paycore.readmodel;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Timestamp;
import com.paycore.common.events.v1.PaymentAuthorized;
import com.paycore.common.events.v1.PaymentCaptured;
import com.paycore.common.events.v1.PaymentEvent;
import com.paycore.common.ids.UuidV7;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
            "paycore.readmodel.kafka-listener-enabled=true",
            "paycore.security.admin-token=readmodel-dlt-test-admin",
            "spring.kafka.consumer.auto-offset-reset=earliest"
        })
@EmbeddedKafka(
        partitions = 1,
        topics = {"payments.events", "payments.events.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@Testcontainers
class ReadModelDeadLetterKafkaIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("paycore_readmodel_dlt_test")
            .withUsername("paycore")
            .withPassword("paycore");

    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void exhaustedProjectionFailureIsRegisteredAndAdminReplayFillsSequenceGap() throws Exception {
        UUID paymentId = UuidV7.generate();
        UUID merchantId = UuidV7.generate();
        Instant now = Instant.parse("2026-10-21T10:00:00Z");
        byte[] failedSequenceOne = authorizedEvent(paymentId, merchantId, "INR1", now);
        byte[] laterSequenceTwo = capturedEvent(paymentId, merchantId, now.plusSeconds(1));

        kafkaTemplate.send("payments.events", paymentId.toString(), laterSequenceTwo).get(10, TimeUnit.SECONDS);
        await(() -> jdbcClient.sql("SELECT COUNT(*) FROM read_model_event_log WHERE payment_id = :id AND event_seq = 2")
                .param("id", paymentId).query(Long.class).single() == 1L, Duration.ofSeconds(10));

        kafkaTemplate.send("payments.events", paymentId.toString(), failedSequenceOne).get(10, TimeUnit.SECONDS);
        await(() -> jdbcClient.sql("SELECT COUNT(*) FROM read_model_dead_letters")
                .query(Long.class).single() == 1L, Duration.ofSeconds(20));

        HttpHeaders adminHeaders = new HttpHeaders();
        adminHeaders.set("X-PayCore-Admin-Token", "readmodel-dlt-test-admin");
        ResponseEntity<ReadModelViews.DeadLetterView[]> listResponse = restTemplate.exchange(
                "/v1/admin/read-model/dead-letters", HttpMethod.GET,
                new HttpEntity<>(adminHeaders), ReadModelViews.DeadLetterView[].class);
        assertThat(listResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listResponse.getBody()).hasSize(1);
        ReadModelViews.DeadLetterView deadLetter = listResponse.getBody()[0];
        assertThat(deadLetter.sourceTopic()).isEqualTo("payments.events");
        assertThat(deadLetter.messageKey()).isEqualTo(paymentId.toString());
        assertThat(deadLetter.exceptionClass()).isNotBlank();

        // Simulate correcting the projection constraint before replaying the unchanged source event.
        jdbcClient.sql("ALTER TABLE payment_view ALTER COLUMN currency TYPE VARCHAR(4)").update();
        ResponseEntity<ReadModelViews.DeadLetterReplayResult> replayResponse = restTemplate.exchange(
                "/v1/admin/read-model/dead-letters/" + deadLetter.id() + "/replay",
                HttpMethod.POST,
                new HttpEntity<>(adminHeaders),
                ReadModelViews.DeadLetterReplayResult.class);
        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        await(() -> jdbcClient.sql("SELECT COUNT(*) FROM payment_view WHERE payment_id = :id AND last_event_seq = 2")
                .param("id", paymentId).query(Long.class).single() == 1L, Duration.ofSeconds(15));
        assertThat(jdbcClient.sql("SELECT status FROM payment_view WHERE payment_id = :id")
                .param("id", paymentId).query(String.class).single()).isEqualTo("CAPTURED");
        assertThat(jdbcClient.sql("SELECT replay_count FROM read_model_dead_letters WHERE id = :id")
                .param("id", deadLetter.id()).query(Integer.class).single()).isEqualTo(1);
    }

    private static byte[] authorizedEvent(UUID paymentId, UUID merchantId, String currency, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(1)
                .setEventType("payment.authorized")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).build())
                .setAuthorized(PaymentAuthorized.newBuilder()
                        .setAmount(10_000L).setCurrency(currency).setPspRef("psp-dlt-test"))
                .build().toByteArray();
    }

    private static byte[] capturedEvent(UUID paymentId, UUID merchantId, Instant at) {
        return PaymentEvent.newBuilder()
                .setEventId(UuidV7.generate().toString())
                .setPaymentId(paymentId.toString())
                .setMerchantId(merchantId.toString())
                .setEventSeq(2)
                .setEventType("payment.captured")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(at.getEpochSecond()).build())
                .setCaptured(PaymentCaptured.newBuilder()
                        .setCapturedAmount(10_000L).setFeeAmount(290L).setCurrency("INR")
                        .setPspRef("psp-dlt-test").setLedgerTxnId(UuidV7.generate().toString()))
                .build().toByteArray();
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(100L);
        }
        assertThat(condition.getAsBoolean()).as("condition should become true within " + timeout).isTrue();
    }
}

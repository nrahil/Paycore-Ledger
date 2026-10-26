package com.paycore.readmodel;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Kafka adapter that invokes the transactional projector through its Spring proxy. */
@Component
public class ReadModelKafkaEventListener {

    public static final String PAYMENTS_EVENTS_TOPIC = "payments.events";

    private final ReadModelProjectorService projectorService;

    public ReadModelKafkaEventListener(ReadModelProjectorService projectorService) {
        this.projectorService = projectorService;
    }

    @KafkaListener(
            topics = PAYMENTS_EVENTS_TOPIC,
            groupId = "read-model-service-group",
            autoStartup = "${paycore.readmodel.kafka-listener-enabled:false}")
    public void onPaymentEvent(byte[] rawPayload) {
        // Call the @Transactional method through its separate bean, so database writes roll back on failure.
        projectorService.ingestEvent(rawPayload);
    }
}

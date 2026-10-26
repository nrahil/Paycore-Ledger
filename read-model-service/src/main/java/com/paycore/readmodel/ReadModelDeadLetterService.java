package com.paycore.readmodel;

import com.paycore.common.clock.ClockProvider;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

/** Stores failed Kafka events and republishes selected records to their keyed source topic. */
@Service
public class ReadModelDeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(ReadModelDeadLetterService.class);

    private final ReadModelDeadLetterRepository repository;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ClockProvider clockProvider;

    public ReadModelDeadLetterService(
            ReadModelDeadLetterRepository repository,
            KafkaTemplate<String, byte[]> kafkaTemplate,
            ClockProvider clockProvider) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.clockProvider = clockProvider;
    }

    @KafkaListener(
            topics = "${paycore.readmodel.dead-letter-topic:payments.events.DLT}",
            groupId = "read-model-dead-letter-registry",
            containerFactory = "readModelDeadLetterListenerContainerFactory",
            autoStartup = "${paycore.readmodel.kafka-listener-enabled:false}")
    @Transactional
    public void recordDeadLetter(ConsumerRecord<String, byte[]> record) {
        repository.save(record, clockProvider.now());
        log.warn("Registered read-model dead letter from topic={} partition={} offset={} key={}",
                record.topic(), record.partition(), record.offset(), record.key());
    }

    public List<ReadModelViews.DeadLetterView> listDeadLetters(int limit) {
        if (limit < 1 || limit > 100) throw new IllegalArgumentException("limit must be between 1 and 100");
        return repository.list(limit);
    }

    public ReadModelViews.DeadLetterReplayResult replay(UUID id) {
        ReadModelDeadLetterRepository.ReadModelDeadLetter deadLetter = repository.find(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Dead-letter event not found"));
        try {
            // Reuse the original payment key. The projector buffers gaps and drains contiguous event_seq values,
            // so replay remains safe even though Kafka appends this record at the current topic tail.
            kafkaTemplate.send(deadLetter.sourceTopic(), deadLetter.messageKey(), deadLetter.rawPayload())
                    .get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Dead-letter replay was interrupted", e);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Failed to replay dead-letter event", e);
        }
        var replayedAt = clockProvider.now();
        repository.markReplayed(id, replayedAt);
        log.info("Replayed read-model dead letter id={} to topic={}", id, deadLetter.sourceTopic());
        return repository.findReplayResult(id, replayedAt);
    }
}

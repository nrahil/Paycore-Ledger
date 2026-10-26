package com.paycore.readmodel;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

/** Kafka retry and dead-letter policies for the read-model consumers. */
@Configuration
public class ReadModelKafkaConfiguration {

    @Bean
    CommonErrorHandler readModelErrorHandler(
            KafkaTemplate<String, byte[]> kafkaTemplate,
            @Value("${paycore.readmodel.dead-letter-topic:payments.events.DLT}") String deadLetterTopic) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, exception) -> new TopicPartition(deadLetterTopic, -1));
        recoverer.setFailIfSendResultIsError(true);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
    }

    @Bean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, byte[]> readModelListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory,
            CommonErrorHandler readModelErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(readModelErrorHandler);
        return factory;
    }

    @Bean(name = "readModelDeadLetterListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<String, byte[]> readModelDeadLetterListenerContainerFactory(
            ConsumerFactory<String, byte[]> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Never acknowledge a dead-letter record until it is durably registered for operator replay.
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(1_000L, FixedBackOff.UNLIMITED_ATTEMPTS)));
        return factory;
    }
}

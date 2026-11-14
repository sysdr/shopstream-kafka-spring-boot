package com.shopstream.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Day 6: a message that keeps failing is retried twice (200 ms apart), then published to
 * orders.DLT with the exception in its headers, and the consumer moves on. A message that can't
 * even be parsed is never retried: it goes straight to the dead-letter topic.
 */
@Configuration
@Profile("fulfillment")
public class DeadLetterConfig {
    @Bean
    public CommonErrorHandler errorHandler(KafkaOperations<String, String> template) {
        // Explicit destination: same partition number in orders.DLT (Spring's default name would be orders-dlt).
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (rec, ex) -> new TopicPartition(Topics.ORDERS_DLT, rec.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2L));
        handler.addNotRetryableExceptions(JsonProcessingException.class);
        return handler;
    }
}

package com.shopstream.events;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/** Attaches the rebalance tracker to the listener container that reads the orders topic. */
@Configuration
@Profile("fulfillment")
public class ListenerConfig {
    @Bean
    public RebalanceTracker rebalanceTracker() {
        return new RebalanceTracker();
    }

    @Bean
    public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> rebalanceCustomizer(
            RebalanceTracker tracker) {
        return container -> {
            String[] topics = container.getContainerProperties().getTopics();
            if (topics != null && java.util.Arrays.asList(topics).contains(Topics.ORDERS)) {
                container.getContainerProperties().setConsumerRebalanceListener(tracker);
            }
        };
    }
}

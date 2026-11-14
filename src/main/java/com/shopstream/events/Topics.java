package com.shopstream.events;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Spring's KafkaAdmin creates these topics at start-up if they do not exist. */
@Configuration
public class Topics {
    public static final String ORDERS = "orders";
    public static final String ORDERS_DLT = "orders.DLT";

    @Bean
    public NewTopic ordersTopic(@Value("${app.orders.partitions:6}") int partitions,
                                @Value("${app.orders.replicas:1}") int replicas,
                                @Value("${app.orders.retention-ms:604800000}") long retentionMs) {
        // Day 4: three copies of every partition; a write needs two of them (min.insync.replicas=2).
        // Day 6: keep 7 days of events, so a consumer that was down over a weekend can still catch up.
        return TopicBuilder.name(ORDERS).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", replicas >= 3 ? "2" : "1")
                .config("retention.ms", String.valueOf(retentionMs))
                .build();
    }

    /** Day 6: messages the consumer cannot process go here instead of blocking the partition. */
    @Bean
    public NewTopic ordersDeadLetterTopic(@Value("${app.orders.partitions:6}") int partitions,
                                          @Value("${app.orders.replicas:1}") int replicas) {
        return TopicBuilder.name(ORDERS_DLT).partitions(partitions).replicas(replicas)
                .config("retention.ms", String.valueOf(30L * 24 * 3600 * 1000)).build();
    }
}

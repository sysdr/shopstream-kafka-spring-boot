package com.shopstream.events;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** Consumer lag = log-end offset minus the group's committed offset, per partition. */
@Component
public class LagService implements AutoCloseable {
    private final AdminClient admin;

    public LagService(KafkaAdmin kafkaAdmin) {
        this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public Map<Integer, Long> lagByPartition(String group, String topic) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetSpec> latest = admin.describeTopics(java.util.List.of(topic)).allTopicNames()
                .get(5, TimeUnit.SECONDS).get(topic).partitions().stream()
                .collect(Collectors.toMap(p -> new TopicPartition(topic, p.partition()), p -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(latest).all()
                .get(5, TimeUnit.SECONDS);
        Map<Integer, Long> lag = new TreeMap<>();
        ends.forEach((tp, end) -> {
            OffsetAndMetadata c = committed.get(tp);
            lag.put(tp.partition(), end.offset() - (c == null ? 0 : c.offset()));
        });
        return lag;
    }

    @Override
    public void close() {
        admin.close();
    }
}

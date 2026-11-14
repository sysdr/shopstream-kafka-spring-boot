package com.shopstream.events;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Who leads each partition, and how many replicas are in sync. */
@RestController
public class ClusterController {
    private final KafkaAdmin kafkaAdmin;

    public ClusterController(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            List<Integer> brokers = admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).stream()
                    .map(Node::id).sorted().toList();
            TopicDescription d = admin.describeTopics(List.of(Topics.ORDERS)).allTopicNames()
                    .get(5, TimeUnit.SECONDS).get(Topics.ORDERS);
            Map<Integer, Integer> leaders = new TreeMap<>();
            Map<Integer, Integer> isr = new TreeMap<>();
            int under = 0;
            for (TopicPartitionInfo p : d.partitions()) {
                leaders.put(p.partition(), p.leader() == null ? -1 : p.leader().id());
                isr.put(p.partition(), p.isr().size());
                if (p.isr().size() < p.replicas().size()) {
                    under++;
                }
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("brokers", brokers);
            out.put("replicas", d.partitions().get(0).replicas().size());
            out.put("leaders", leaders);
            out.put("isr", isr);
            out.put("underReplicated", under);
            return out;
        }
    }
}

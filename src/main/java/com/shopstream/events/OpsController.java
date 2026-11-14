package com.shopstream.events;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Day 6 operations endpoints on order-service: topic settings and a poison-message injector. */
@RestController
@Profile("orders")
public class OpsController {
    private final KafkaAdmin kafkaAdmin;
    private final KafkaTemplate<String, String> kafka;

    public OpsController(KafkaAdmin kafkaAdmin, KafkaTemplate<String, String> kafka) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafka = kafka;
    }

    @GetMapping("/ops/topic")
    public Map<String, Object> topic() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, Topics.ORDERS);
            Config c = admin.describeConfigs(List.of(r)).all().get(5, TimeUnit.SECONDS).get(r);
            Map<String, Object> out = new LinkedHashMap<>();
            for (String k : List.of("retention.ms", "min.insync.replicas", "cleanup.policy", "segment.bytes")) {
                out.put(k, c.get(k).value());
            }
            return out;
        }
    }

    /** Publishes messages that are not valid OrderEvent JSON, to exercise the dead-letter topic. */
    @PostMapping("/ops/poison")
    public Map<String, Object> poison(@RequestParam(defaultValue = "1") int count) throws Exception {
        for (int i = 0; i < count; i++) {
            kafka.send(Topics.ORDERS, "POISON-" + i, "{not json: order " + i).get(10, TimeUnit.SECONDS);
        }
        return Map.of("sent", count);
    }
}

package com.shopstream.events;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Three producer configurations, side by side, so the same burst can be replayed with each:
 *   acks1    acks=1, no idempotence: the leader alone confirms the write
 *   acksAll  acks=all, no idempotence: every in-sync replica has it, but retries can duplicate
 *   safe     acks=all + enable.idempotence=true: no loss, no duplicates on retry
 */
@Component
@Profile("orders")
public class ProducerModes {
    public static final String[] MODES = {"acks1", "acksAll", "safe"};
    private final Map<String, KafkaTemplate<String, String>> templates = new LinkedHashMap<>();

    public ProducerModes(@Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        for (String mode : MODES) {
            templates.put(mode, new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config(mode, bootstrap))));
        }
    }

    public static Map<String, Object> config(String mode, String bootstrap) {
        Map<String, Object> c = new HashMap<>();
        c.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        c.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        c.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        c.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        switch (mode) {
            case "acks1" -> {
                c.put(ProducerConfig.ACKS_CONFIG, "1");
                c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case "acksAll" -> {
                c.put(ProducerConfig.ACKS_CONFIG, "all");
                c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case "safe" -> {
                c.put(ProducerConfig.ACKS_CONFIG, "all");
                c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                c.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            }
            default -> throw new IllegalArgumentException("unknown producer mode " + mode);
        }
        return c;
    }

    public KafkaTemplate<String, String> template(String mode) {
        KafkaTemplate<String, String> t = templates.get(mode);
        if (t == null) {
            throw new IllegalArgumentException("unknown producer mode " + mode);
        }
        return t;
    }
}

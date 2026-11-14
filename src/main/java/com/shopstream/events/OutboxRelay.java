package com.shopstream.events;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Day 5: reads unpublished outbox rows in order, publishes each, then marks it published.
 * A crash after the publish and before the mark re-publishes that row on restart:
 * at-least-once delivery, so the consumer must be idempotent.
 */
@Component
@Profile("orders")
public class OutboxRelay {
    private final JdbcTemplate db;
    private final KafkaTemplate<String, String> kafka;
    private final CrashSwitch crash;

    public OutboxRelay(JdbcTemplate db, KafkaTemplate<String, String> kafka, CrashSwitch crash) {
        this.db = db;
        this.kafka = kafka;
        this.crash = crash;
    }

    @Scheduled(fixedDelay = 100)
    public void relay() throws Exception {
        List<Map<String, Object>> rows = db.queryForList(
                "SELECT id, aggregate_id, payload FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT 200");
        for (Map<String, Object> row : rows) {
            String key = (String) row.get("AGGREGATE_ID");
            kafka.send(Topics.ORDERS, key, (String) row.get("PAYLOAD")).get(10, TimeUnit.SECONDS);
            crash.maybe("after publishing outbox row " + row.get("ID") + ", before marking it published");
            db.update("UPDATE outbox SET published_at = CURRENT_TIMESTAMP WHERE id = ?", row.get("ID"));
        }
    }
}

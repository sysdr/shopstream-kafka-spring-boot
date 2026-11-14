package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Day 5: two ways to save an order and announce it.
 *   dual   - commit to the database, then publish to Kafka (two writes, no atomicity)
 *   outbox - commit the order AND its event row in one transaction; OutboxRelay publishes later
 */
@Service
@Profile("orders")
public class OrderStore {
    private final JdbcTemplate db;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final CrashSwitch crash;

    public OrderStore(JdbcTemplate db, TransactionTemplate tx, KafkaTemplate<String, String> kafka, ObjectMapper json,
                      CrashSwitch crash) {
        this.db = db;
        this.tx = tx;
        this.kafka = kafka;
        this.json = json;
        this.crash = crash;
    }

    private OrderEvent newOrder(String prefix, int i) {
        long n = db.queryForObject("SELECT NEXT VALUE FOR order_seq", Long.class);
        String orderId = String.format("%s-%06d", prefix, n);
        return new OrderEvent(UUID.randomUUID().toString(), orderId, "OrderPlaced", 1, "C-" + (i % 50),
                "SKU-" + (i % 20), 1 + i % 3, Math.round((1 + i % 3) * 19.99 * 100) / 100.0, System.currentTimeMillis());
    }

    private void insertOrder(OrderEvent e) {
        db.update("INSERT INTO orders (order_id, customer_id, sku, quantity, total) VALUES (?, ?, ?, ?, ?)",
                e.orderId(), e.customerId(), e.sku(), e.quantity(), e.total());
    }

    /** The naive way: two separate writes. A crash between them loses the event for good. */
    public String placeDualWrite(int i) throws Exception {
        OrderEvent e = newOrder("DUAL", i);
        tx.executeWithoutResult(s -> insertOrder(e));
        crash.maybe("after the database commit, before the Kafka publish (" + e.orderId() + ")");
        kafka.send(Topics.ORDERS, e.orderId(), json.writeValueAsString(e)).get(10, TimeUnit.SECONDS);
        return e.orderId();
    }

    /** The outbox way: the order and its event are one atomic database write. */
    public List<String> placeWithOutbox(int count) {
        return tx.execute(s -> {
            List<String> ids = new java.util.ArrayList<>();
            for (int i = 0; i < count; i++) {
                OrderEvent e = newOrder("OBX", i);
                insertOrder(e);
                try {
                    db.update("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (?, ?, ?)",
                            e.orderId(), e.type(), json.writeValueAsString(e));
                } catch (Exception ex) {
                    throw new IllegalStateException(ex);
                }
                ids.add(e.orderId());
            }
            return ids;
        });
    }

    public List<String> orderIds(String prefix) {
        return db.queryForList("SELECT order_id FROM orders WHERE order_id LIKE ? ORDER BY order_id", String.class,
                prefix + "-%");
    }

    public Map<String, Object> outboxStatus() {
        return db.queryForMap("SELECT COUNT(*) AS total, COUNT(published_at) AS published, "
                + "COUNT(*) - COUNT(published_at) AS pending FROM outbox");
    }
}

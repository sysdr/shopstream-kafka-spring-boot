package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** order-service: publishes order events to the "orders" topic. */
@RestController
@Profile("orders")
public class OrderController {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final int partitions;
    private final AtomicLong nextId = new AtomicLong(1001);

    public OrderController(KafkaTemplate<String, String> kafka, ObjectMapper json,
                           @Value("${app.orders.partitions:6}") int partitions) {
        this.kafka = kafka;
        this.json = json;
        this.partitions = partitions;
    }

    public record NewOrder(String customerId, String sku, int quantity, double price) {
    }

    public record Simulation(int orders, String keyMode) {
    }

    private OrderEvent event(String orderId, int seq, NewOrder o) {
        return new OrderEvent(UUID.randomUUID().toString(), orderId, OrderEvent.LIFECYCLE[seq - 1], seq,
                o.customerId(), o.sku(), o.quantity(), Math.round(o.quantity() * o.price() * 100) / 100.0,
                System.currentTimeMillis());
    }

    private CompletableFuture<SendResult<String, String>> publish(OrderEvent e, String keyMode) throws Exception {
        return kafka.send(Topics.ORDERS, Keys.keyFor(e, keyMode), json.writeValueAsString(e));
    }

    /** Day 1: one OrderPlaced, keyed by order id; waits for the broker's acknowledgement. */
    @PostMapping("/orders")
    public Map<String, Object> place(@RequestBody NewOrder req) throws Exception {
        String orderId = "ORD-" + nextId.getAndIncrement();
        SendResult<String, String> sent = publish(event(orderId, 1, req), "orderId").get(10, TimeUnit.SECONDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("orderId", orderId);
        out.put("partition", sent.getRecordMetadata().partition());
        out.put("offset", sent.getRecordMetadata().offset());
        return out;
    }

    /** Day 2: N orders, each Placed then Paid then Shipped, published back to back with the chosen key. */
    @PostMapping("/orders/simulate")
    public Map<String, Object> simulate(@RequestBody Simulation sim) throws Exception {
        String mode = sim.keyMode() == null ? "orderId" : sim.keyMode();
        long t0 = System.nanoTime();
        List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>();
        for (int i = 0; i < sim.orders(); i++) {
            String orderId = "ORD-" + nextId.getAndIncrement();
            NewOrder o = new NewOrder("C-" + (i % 50), "SKU-" + (i % 20), 1 + i % 3, 19.99);
            for (int seq = 1; seq <= 3; seq++) {
                sends.add(publish(event(orderId, seq, o), mode));
            }
        }
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("orders", sim.orders());
        out.put("events", sends.size());
        out.put("keyMode", mode);
        out.put("ms", (System.nanoTime() - t0) / 1_000_000);
        return out;
    }

    /** Which partition an order id maps to (same murmur2 formula as the producer). */
    @GetMapping("/orders/{orderId}/partition")
    public Map<String, Object> partition(@PathVariable String orderId) {
        return Map.of("orderId", orderId, "partition", Keys.partitionFor(orderId, partitions), "partitions", partitions);
    }
}

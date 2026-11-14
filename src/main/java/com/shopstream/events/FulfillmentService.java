package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** fulfillment-service: one member of the "fulfillment" consumer group. Run several to scale out. */
@RestController
@Profile("fulfillment")
public class FulfillmentService {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);
    private final ObjectMapper json;
    private final LatencyStats latency = new LatencyStats();
    private final OrderTracker tracker = new OrderTracker();
    private final RunTracker runs = new RunTracker();
    private final RebalanceTracker rebalances;
    private final LagService lag;
    private final long processDelayMs;
    private final String instance;

    public FulfillmentService(ObjectMapper json, RebalanceTracker rebalances, LagService lag,
                              @Value("${app.process-delay-ms:1}") long processDelayMs,
                              @Value("${app.instance:f1}") String instance) {
        this.json = json;
        this.rebalances = rebalances;
        this.lag = lag;
        this.processDelayMs = processDelayMs;
        this.instance = instance;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "fulfillment", concurrency = "${app.consumer.concurrency:1}")
    public void onEvent(ConsumerRecord<String, String> rec) throws Exception {
        OrderEvent e = json.readValue(rec.value(), OrderEvent.class);
        latency.record(e.orderId(), System.currentTimeMillis() - e.createdAtMs());
        runs.record(e.orderId());
        if (!tracker.record(e, rec.partition())) {
            log.warn("{} {} (seq {}) arrived out of order on partition {}", e.orderId(), e.type(), e.seq(), rec.partition());
        }
        if (processDelayMs > 0) {
            Thread.sleep(processDelayMs); // stands in for real work: reserve stock, print a label
        }
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instance", instance);
        out.putAll(latency.snapshot());
        out.putAll(tracker.snapshot());
        out.putAll(rebalances.snapshot());
        return out;
    }

    @PostMapping("/stats/reset")
    public Map<String, Object> reset() {
        latency.reset();
        tracker.reset();
        return stats();
    }

    @GetMapping("/lag")
    public Map<String, Object> lag() throws Exception {
        Map<Integer, Long> byPartition = lag.lagByPartition("fulfillment", Topics.ORDERS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total", byPartition.values().stream().mapToLong(Long::longValue).sum());
        out.put("byPartition", byPartition);
        return out;
    }

    /** Day 4: what arrived for one burst (see BurstService). */
    @GetMapping("/runs/{runId}")
    public Map<String, Object> run(@PathVariable String runId, @RequestParam(defaultValue = "false") boolean ids) {
        return runs.snapshot(runId, ids);
    }
}

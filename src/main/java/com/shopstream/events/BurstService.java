package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Day 4: publish a paced burst of orders with one producer mode and remember exactly which
 * order ids the brokers acknowledged. Those acknowledged ids are promises: every one must reach
 * the consumer.
 */
@RestController
@Profile("orders")
public class BurstService {
    private final ProducerModes modes;
    private final ObjectMapper json;
    private final ExecutorService runner = Executors.newSingleThreadExecutor();
    private final Map<String, Run> runs = new ConcurrentHashMap<>();

    public BurstService(ProducerModes modes, ObjectMapper json) {
        this.modes = modes;
        this.json = json;
    }

    public record Burst(String runId, String mode, int count, int ratePerSec) {
    }

    static final class Run {
        final Set<String> acked = ConcurrentHashMap.newKeySet();
        final AtomicInteger failed = new AtomicInteger();
        final AtomicInteger sent = new AtomicInteger();
        volatile boolean done;
        volatile long ms;
        volatile String error = "";
        volatile long lastAckMs;
        volatile long maxAckGapMs;

        synchronized void ack(String orderId) {
            long now = System.currentTimeMillis();
            if (lastAckMs > 0) {
                maxAckGapMs = Math.max(maxAckGapMs, now - lastAckMs);
            }
            lastAckMs = now;
            acked.add(orderId);
        }
    }

    @PostMapping("/bursts")
    public Map<String, Object> start(@RequestBody Burst b) {
        modes.template(b.mode()); // fail fast on a bad mode
        Run run = new Run();
        runs.put(b.runId(), run);
        runner.submit(() -> publish(b, run));
        return Map.of("runId", b.runId(), "started", true);
    }

    private void publish(Burst b, Run run) {
        long t0 = System.nanoTime();
        long perEventNanos = 1_000_000_000L / Math.max(1, b.ratePerSec());
        try {
            for (int i = 1; i <= b.count(); i++) {
                String orderId = String.format("%s-%06d", b.runId(), i);
                OrderEvent e = new OrderEvent(UUID.randomUUID().toString(), orderId, "OrderPlaced", 1,
                        "C-" + (i % 50), "SKU-" + (i % 20), 1, 19.99, System.currentTimeMillis());
                modes.template(b.mode()).send(Topics.ORDERS, orderId, json.writeValueAsString(e))
                        .whenComplete((res, ex) -> {
                            if (ex == null) {
                                run.ack(orderId);
                            } else {
                                run.failed.incrementAndGet();
                            }
                        });
                run.sent.incrementAndGet();
                long due = t0 + i * perEventNanos;
                long wait = due - System.nanoTime();
                if (wait > 0) {
                    Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
            }
            modes.template(b.mode()).flush();
            // wait for the last callbacks (bounded by delivery.timeout.ms)
            long end = System.currentTimeMillis() + 35_000;
            while (run.acked.size() + run.failed.get() < b.count() && System.currentTimeMillis() < end) {
                Thread.sleep(50);
            }
        } catch (Exception ex) {
            run.error = ex.toString();
        }
        run.ms = (System.nanoTime() - t0) / 1_000_000;
        run.done = true;
    }

    @GetMapping("/bursts/{runId}")
    public Map<String, Object> status(@PathVariable String runId, @RequestParam(defaultValue = "false") boolean ids) {
        Run run = runs.get(runId);
        if (run == null) {
            return Map.of("error", "no such run");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runId", runId);
        out.put("done", run.done);
        out.put("sent", run.sent.get());
        out.put("acked", run.acked.size());
        out.put("failed", run.failed.get());
        out.put("ms", run.ms);
        out.put("maxAckGapMs", run.maxAckGapMs);
        out.put("error", run.error);
        if (ids) {
            out.put("ackedIds", run.acked);
        }
        return out;
    }
}

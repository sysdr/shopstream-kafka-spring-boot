package com.shopstream.events;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Day 6: a lag alert with a "for" duration, like a Prometheus rule:
 *   OK -> PENDING when lag > threshold; PENDING -> FIRING after forSeconds; back to OK when lag <= threshold.
 * It runs inside order-service here because that service stays up while fulfillment is down.
 */
@RestController
@Profile("orders")
public class LagAlert {
    private static final Logger log = LoggerFactory.getLogger(LagAlert.class);
    private final LagService lagService;
    private final long threshold;
    private final long forMs;
    private final List<String> transitions = new ArrayList<>();
    private String state = "OK";
    private long aboveSince = -1;
    private long lastLag;

    public LagAlert(LagService lagService, @Value("${app.alert.lag-threshold:500}") long threshold,
                    @Value("${app.alert.for-seconds:5}") long forSeconds) {
        this.lagService = lagService;
        this.threshold = threshold;
        this.forMs = forSeconds * 1000;
    }

    /** Pure state machine, so it can be unit-tested with a fake clock. */
    public synchronized String evaluate(long lag, long nowMs) {
        lastLag = lag;
        String next = state;
        if (lag <= threshold) {
            aboveSince = -1;
            next = "OK";
        } else {
            if (aboveSince < 0) {
                aboveSince = nowMs;
            }
            next = nowMs - aboveSince >= forMs ? "FIRING" : "PENDING";
        }
        if (!next.equals(state)) {
            transitions.add(Instant.ofEpochMilli(nowMs) + " " + state + " -> " + next + " (lag " + lag + ")");
            if ("FIRING".equals(next)) {
                log.warn("ALERT FulfillmentLagHigh: lag {} above {} for {} s", lag, threshold, forMs / 1000);
            }
            state = next;
        }
        return state;
    }

    @Scheduled(fixedDelay = 1000)
    public void tick() {
        try {
            long lag = lagService.lagByPartition("fulfillment", Topics.ORDERS).values().stream()
                    .mapToLong(Long::longValue).sum();
            evaluate(lag, System.currentTimeMillis());
        } catch (Exception e) {
            log.debug("lag check failed: {}", e.toString());
        }
    }

    @GetMapping("/ops/alert")
    public synchronized Map<String, Object> alert() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("name", "FulfillmentLagHigh");
        out.put("state", state);
        out.put("lag", lastLag);
        out.put("threshold", threshold);
        out.put("forSeconds", forMs / 1000);
        out.put("transitions", List.copyOf(transitions));
        return out;
    }
}

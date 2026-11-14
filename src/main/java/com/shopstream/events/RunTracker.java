package com.shopstream.events;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Consumer side of Day 4: per run, how many events arrived and how many distinct orders. */
public class RunTracker {
    private final Map<String, AtomicInteger> received = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> unique = new ConcurrentHashMap<>();

    /** Burst order ids look like "safe-000123"; everything before the last '-' is the run id. */
    public static String runOf(String orderId) {
        int i = orderId.lastIndexOf('-');
        return i < 0 ? orderId : orderId.substring(0, i);
    }

    public void record(String orderId) {
        String run = runOf(orderId);
        received.computeIfAbsent(run, k -> new AtomicInteger()).incrementAndGet();
        unique.computeIfAbsent(run, k -> ConcurrentHashMap.newKeySet()).add(orderId);
    }

    public Map<String, Object> snapshot(String run, boolean ids) {
        Map<String, Object> out = new LinkedHashMap<>();
        int r = received.getOrDefault(run, new AtomicInteger()).get();
        Set<String> u = unique.getOrDefault(run, Set.of());
        out.put("runId", run);
        out.put("received", r);
        out.put("unique", u.size());
        out.put("duplicates", r - u.size());
        if (ids) {
            out.put("ids", u);
        }
        return out;
    }
}

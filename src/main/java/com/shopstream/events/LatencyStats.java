package com.shopstream.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Thread-safe store of end-to-end latencies (producer timestamp to consumer receive). */
public class LatencyStats {
    private final List<Long> samples = new ArrayList<>();
    private String lastOrderId = "";

    public synchronized void record(String orderId, long latencyMs) {
        samples.add(Math.max(0, latencyMs));
        lastOrderId = orderId;
    }

    public synchronized void reset() {
        samples.clear();
        lastOrderId = "";
    }

    public synchronized int count() {
        return samples.size();
    }

    /** Nearest-rank percentile; 0 when there are no samples. */
    public synchronized long percentile(double p) {
        if (samples.isEmpty()) {
            return 0;
        }
        List<Long> sorted = new ArrayList<>(samples);
        Collections.sort(sorted);
        int rank = (int) Math.ceil(p / 100.0 * sorted.size());
        return sorted.get(Math.min(sorted.size() - 1, Math.max(0, rank - 1)));
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("received", samples.size());
        out.put("p50Ms", percentile(50));
        out.put("p99Ms", percentile(99));
        out.put("maxMs", samples.isEmpty() ? 0 : Collections.max(samples));
        out.put("lastOrderId", lastOrderId);
        return out;
    }
}

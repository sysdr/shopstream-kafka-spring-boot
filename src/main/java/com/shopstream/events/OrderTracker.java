package com.shopstream.events;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Checks each order's events arrive in lifecycle order (seq 1, 2, 3) and records which
 * partitions carried them. Thread-safe: several listener threads call record().
 */
public class OrderTracker {
    private final Map<String, Integer> lastSeq = new HashMap<>();
    private final Map<String, Set<Integer>> partitionsByOrder = new HashMap<>();
    private final Map<Integer, Integer> eventsByPartition = new TreeMap<>();
    private final Set<String> disordered = new HashSet<>();
    private int received;
    private int outOfOrder;
    private int completed;

    /** Returns true when the event arrived in order. */
    public synchronized boolean record(OrderEvent e, int partition) {
        received++;
        eventsByPartition.merge(partition, 1, Integer::sum);
        partitionsByOrder.computeIfAbsent(e.orderId(), k -> new HashSet<>()).add(partition);
        int last = lastSeq.getOrDefault(e.orderId(), 0);
        boolean inOrder = e.seq() == last + 1;
        if (!inOrder) {
            outOfOrder++;
            disordered.add(e.orderId());
        }
        lastSeq.put(e.orderId(), Math.max(last, e.seq()));
        if (e.seq() == 3 && inOrder && !disordered.contains(e.orderId())) {
            completed++;
        }
        return inOrder;
    }

    public synchronized void reset() {
        lastSeq.clear();
        partitionsByOrder.clear();
        disordered.clear();
        eventsByPartition.clear();
        received = 0;
        outOfOrder = 0;
        completed = 0;
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("events", received);
        out.put("orders", lastSeq.size());
        out.put("outOfOrder", outOfOrder);
        out.put("ordersShippedInOrder", completed);
        out.put("ordersSplitAcrossPartitions", partitionsByOrder.values().stream().filter(s -> s.size() > 1).count());
        out.put("eventsByPartition", new TreeMap<>(eventsByPartition));
        return out;
    }
}

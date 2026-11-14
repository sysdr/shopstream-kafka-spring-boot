package com.shopstream.events;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/** Counts rebalances and remembers which partitions this instance owns right now. */
public class RebalanceTracker implements ConsumerAwareRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(RebalanceTracker.class);
    private final TreeSet<Integer> owned = new TreeSet<>();
    private int assignments;
    private int revocations;
    private int lost;

    @Override
    public synchronized void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        assignments++;
        partitions.forEach(tp -> owned.add(tp.partition()));
        log.info("rebalance #{}: assigned {} -> now own {}", assignments, partitions, owned);
    }

    @Override
    public synchronized void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        revocations++;
        partitions.forEach(tp -> owned.remove(tp.partition()));
    }

    @Override
    public synchronized void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        lost++;
        partitions.forEach(tp -> owned.remove(tp.partition()));
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("rebalances", assignments);
        out.put("revocations", revocations);
        out.put("partitionsLost", lost);
        out.put("ownedPartitions", new TreeSet<>(owned));
        return out;
    }
}

package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class RebalanceTrackerTest {
    @Test
    void tracksOwnedPartitionsAcrossRebalances() {
        RebalanceTracker t = new RebalanceTracker();
        t.onPartitionsAssigned(null, List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1)));
        t.onPartitionsRevokedBeforeCommit(null, List.of(new TopicPartition("orders", 1)));
        t.onPartitionsAssigned(null, List.of(new TopicPartition("orders", 4)));
        assertEquals(2, t.snapshot().get("rebalances"));
        assertEquals(Set.of(0, 4), t.snapshot().get("ownedPartitions"));
        t.onPartitionsLost(null, List.of(new TopicPartition("orders", 0)));
        assertEquals(Set.of(4), t.snapshot().get("ownedPartitions"));
    }
}

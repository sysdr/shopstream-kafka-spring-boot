package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class KeysTest {
    private OrderEvent ev(String eventId, int seq) {
        return new OrderEvent(eventId, "ORD-1001", OrderEvent.LIFECYCLE[seq - 1], seq, "C-1", "SKU-1", 1, 9.99, 0);
    }

    @Test
    void orderIdKeySendsEveryEventOfAnOrderToOnePartition() {
        assertEquals(Keys.keyFor(ev("a", 1), "orderId"), Keys.keyFor(ev("b", 2), "orderId"));
        assertEquals(Keys.partitionFor("ORD-1001", 6), Keys.partitionFor(Keys.keyFor(ev("c", 3), "orderId"), 6));
    }

    @Test
    void eventIdKeyChangesWithEveryEvent() {
        assertNotEquals(Keys.keyFor(ev("a", 1), "eventId"), Keys.keyFor(ev("b", 2), "eventId"));
    }

    @Test
    void partitionIsStableAndInRange() {
        int p = Keys.partitionFor("ORD-1001", 6);
        assertEquals(p, Keys.partitionFor("ORD-1001", 6));
        for (int i = 0; i < 1000; i++) {
            int q = Keys.partitionFor("ORD-" + i, 6);
            assertEquals(true, q >= 0 && q < 6);
        }
    }
}

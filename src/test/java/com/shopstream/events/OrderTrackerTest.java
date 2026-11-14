package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OrderTrackerTest {
    private OrderEvent ev(String order, int seq) {
        return new OrderEvent(order + "-" + seq, order, OrderEvent.LIFECYCLE[seq - 1], seq, "C-1", "SKU-1", 1, 9.99, 0);
    }

    @Test
    void inOrderEventsCompleteTheOrder() {
        OrderTracker t = new OrderTracker();
        t.record(ev("ORD-1", 1), 2);
        t.record(ev("ORD-1", 2), 2);
        t.record(ev("ORD-1", 3), 2);
        assertEquals(0, t.snapshot().get("outOfOrder"));
        assertEquals(1, t.snapshot().get("ordersShippedInOrder"));
        assertEquals(0L, t.snapshot().get("ordersSplitAcrossPartitions"));
    }

    @Test
    void paidBeforePlacedIsCountedOutOfOrder() {
        OrderTracker t = new OrderTracker();
        t.record(ev("ORD-2", 2), 0);
        t.record(ev("ORD-2", 1), 4);
        t.record(ev("ORD-2", 3), 1);
        assertEquals(2, t.snapshot().get("outOfOrder"));
        assertEquals(0, t.snapshot().get("ordersShippedInOrder"));
        assertEquals(1L, t.snapshot().get("ordersSplitAcrossPartitions"));
    }
}

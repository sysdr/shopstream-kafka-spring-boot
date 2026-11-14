package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LatencyStatsTest {
    @Test
    void emptyStatsAreZero() {
        LatencyStats s = new LatencyStats();
        assertEquals(0, s.count());
        assertEquals(0, s.percentile(99));
    }

    @Test
    void nearestRankPercentiles() {
        LatencyStats s = new LatencyStats();
        for (int i = 1; i <= 100; i++) {
            s.record("ORD-" + i, i);
        }
        assertEquals(50, s.percentile(50));
        assertEquals(99, s.percentile(99));
        assertEquals(100, s.percentile(100));
        assertEquals("ORD-100", s.snapshot().get("lastOrderId"));
    }

    @Test
    void negativeClockSkewCountsAsZero() {
        LatencyStats s = new LatencyStats();
        s.record("ORD-1", -5);
        assertEquals(0, s.percentile(50));
    }
}

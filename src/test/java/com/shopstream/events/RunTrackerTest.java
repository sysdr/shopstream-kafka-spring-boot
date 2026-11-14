package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RunTrackerTest {
    @Test
    void countsDuplicatesPerRun() {
        RunTracker t = new RunTracker();
        t.record("safe-000001");
        t.record("safe-000002");
        t.record("safe-000002");
        t.record("acks1-000001");
        assertEquals(3, t.snapshot("safe", false).get("received"));
        assertEquals(2, t.snapshot("safe", false).get("unique"));
        assertEquals(1, t.snapshot("safe", false).get("duplicates"));
        assertEquals(1, t.snapshot("acks1", false).get("unique"));
        assertEquals("ORD", RunTracker.runOf("ORD-1001"));
    }
}

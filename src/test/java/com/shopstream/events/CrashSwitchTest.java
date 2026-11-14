package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class CrashSwitchTest {
    @Test
    void crashesOnEveryNthHit() {
        CrashSwitch c = new CrashSwitch(3);
        assertFalse(c.shouldCrash());
        assertFalse(c.shouldCrash());
        assertEquals(true, c.shouldCrash());
        assertFalse(c.shouldCrash());
    }

    @Test
    void zeroNeverCrashes() {
        CrashSwitch c = new CrashSwitch(0);
        for (int i = 0; i < 100; i++) {
            assertFalse(c.shouldCrash());
        }
    }
}

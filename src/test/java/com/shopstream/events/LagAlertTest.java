package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LagAlertTest {
    @Test
    void firesOnlyAfterTheForDurationAndClears() {
        LagAlert a = new LagAlert(null, 500, 5);
        assertEquals("OK", a.evaluate(100, 0));
        assertEquals("PENDING", a.evaluate(800, 1000));
        assertEquals("PENDING", a.evaluate(900, 5000));
        assertEquals("FIRING", a.evaluate(900, 6000));
        assertEquals("OK", a.evaluate(10, 9000));
        assertEquals(3, ((java.util.List<?>) a.alert().get("transitions")).size());
    }

    @Test
    void aShortSpikeNeverFires() {
        LagAlert a = new LagAlert(null, 500, 5);
        a.evaluate(5000, 0);
        a.evaluate(5000, 3000);
        assertEquals("OK", a.evaluate(0, 4000));
        assertEquals("OK", a.evaluate(0, 20000));
    }
}

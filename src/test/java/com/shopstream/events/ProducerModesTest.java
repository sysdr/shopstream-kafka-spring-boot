package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class ProducerModesTest {
    @Test
    void safeModeWaitsForAllReplicasAndIsIdempotent() {
        Map<String, Object> c = ProducerModes.config("safe", "localhost:19092");
        assertEquals("all", c.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, c.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    void unsafeModesSayWhatTheyGiveUp() {
        assertEquals("1", ProducerModes.config("acks1", "x").get(ProducerConfig.ACKS_CONFIG));
        assertEquals(false, ProducerModes.config("acksAll", "x").get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    void unknownModeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ProducerModes.config("yolo", "x"));
    }
}

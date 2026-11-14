package com.shopstream.events;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.utils.Utils;

/** Which key an event is published with decides its partition, and so its order. */
public final class Keys {
    private Keys() {
    }

    /** "orderId" (correct: one order, one partition) or "eventId" (the bug: every event lands anywhere). */
    public static String keyFor(OrderEvent e, String mode) {
        return "eventId".equals(mode) ? e.eventId() : e.orderId();
    }

    /** The same formula Kafka's default partitioner uses for a non-null key. */
    public static int partitionFor(String key, int partitions) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
    }
}

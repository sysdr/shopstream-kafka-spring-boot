package com.shopstream.events;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Day 6: reads orders.DLT so an operator can see what failed and why. */
@RestController
@Profile("fulfillment")
public class DeadLetterInspector {
    private final Deque<Map<String, Object>> recent = new ArrayDeque<>();
    private int count;

    @KafkaListener(topics = Topics.ORDERS_DLT, groupId = "dlt-inspector")
    public synchronized void onDeadLetter(ConsumerRecord<String, String> rec) {
        count++;
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("key", rec.key());
        m.put("value", rec.value() == null ? null : rec.value().substring(0, Math.min(60, rec.value().length())));
        m.put("originalPartition", header(rec, KafkaHeaders.DLT_ORIGINAL_PARTITION, true));
        m.put("originalOffset", header(rec, KafkaHeaders.DLT_ORIGINAL_OFFSET, true));
        String cause = (String) header(rec, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, false);
        m.put("cause", cause == null ? null : cause.substring(cause.lastIndexOf('.') + 1));
        String reason = (String) header(rec, KafkaHeaders.DLT_EXCEPTION_MESSAGE, false);
        m.put("reason", reason == null ? null : reason.substring(0, Math.min(200, reason.length())));
        recent.addFirst(m);
        while (recent.size() > 10) {
            recent.removeLast();
        }
    }

    private static Object header(ConsumerRecord<String, String> rec, String name, boolean numeric) {
        Header h = rec.headers().lastHeader(name);
        if (h == null) {
            return null;
        }
        if (numeric) {
            byte[] b = h.value();
            return b.length == 4 ? java.nio.ByteBuffer.wrap(b).getInt() : java.nio.ByteBuffer.wrap(b).getLong();
        }
        return new String(h.value(), StandardCharsets.UTF_8);
    }

    @GetMapping("/ops/dlt")
    public synchronized Map<String, Object> dlt() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("deadLetters", count);
        out.put("recent", List.copyOf(recent));
        return out;
    }
}

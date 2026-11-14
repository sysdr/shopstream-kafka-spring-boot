package com.shopstream.events;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Day 5 endpoints: place orders with either strategy, and inspect the database side. */
@RestController
@Profile("orders")
public class OutboxController {
    private final OrderStore store;

    public OutboxController(OrderStore store) {
        this.store = store;
    }

    public record Batch(String mode, int count) {
    }

    @PostMapping("/orders/batch")
    public Map<String, Object> batch(@RequestBody Batch b) throws Exception {
        List<String> ids = new ArrayList<>();
        if ("outbox".equals(b.mode())) {
            ids.addAll(store.placeWithOutbox(b.count()));
        } else {
            for (int i = 0; i < b.count(); i++) {
                ids.add(store.placeDualWrite(i));
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("mode", b.mode());
        out.put("placed", ids.size());
        return out;
    }

    @GetMapping("/orders/ids")
    public List<String> ids(@RequestParam String prefix) {
        return store.orderIds(prefix);
    }

    @GetMapping("/outbox")
    public Map<String, Object> outbox() {
        return store.outboxStatus();
    }
}

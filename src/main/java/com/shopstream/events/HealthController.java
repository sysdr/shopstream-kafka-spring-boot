package com.shopstream.events;

import java.util.Map;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {
    private final Environment env;

    public HealthController(Environment env) {
        this.env = env;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "ok", "role", String.join(",", env.getActiveProfiles()));
    }
}

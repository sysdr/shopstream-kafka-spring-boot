package com.shopstream.events;

import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Day 5 fault injection. With app.crash.every-n=N the process dies (like kill -9: no shutdown
 * hooks, no finally blocks) the Nth time it reaches a crash point. 0 disables it.
 */
@Component
public class CrashSwitch {
    private static final Logger log = LoggerFactory.getLogger(CrashSwitch.class);
    private final int everyN;
    private final AtomicInteger hits = new AtomicInteger();

    public CrashSwitch(@Value("${app.crash.every-n:0}") int everyN) {
        this.everyN = everyN;
    }

    /** True when this hit should crash; separated from maybe() so it can be unit-tested. */
    public boolean shouldCrash() {
        return everyN > 0 && hits.incrementAndGet() % everyN == 0;
    }

    public void maybe(String where) {
        if (shouldCrash()) {
            log.error("CRASH {}", where);
            Runtime.getRuntime().halt(137);
        }
    }
}

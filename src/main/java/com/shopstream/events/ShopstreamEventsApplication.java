package com.shopstream.events;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * One jar, two services. Pick the role with a Spring profile:
 *   --spring.profiles.active=orders       order-service: POST /orders publishes OrderPlaced
 *   --spring.profiles.active=fulfillment  fulfillment-service: consumes OrderPlaced
 */
@SpringBootApplication
@EnableScheduling
public class ShopstreamEventsApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShopstreamEventsApplication.class, args);
    }
}

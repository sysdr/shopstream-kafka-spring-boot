package com.shopstream.events;

/**
 * One step in an order's life: OrderPlaced (seq 1), OrderPaid (seq 2), OrderShipped (seq 3).
 * createdAtMs is the producer's clock, used to measure end-to-end latency.
 */
public record OrderEvent(String eventId, String orderId, String type, int seq, String customerId, String sku,
                         int quantity, double total, long createdAtMs) {
    public static final String[] LIFECYCLE = {"OrderPlaced", "OrderPaid", "OrderShipped"};
}

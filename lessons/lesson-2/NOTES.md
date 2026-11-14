Every ShopStream order produces three events: **OrderPlaced**, **OrderPaid**, **OrderShipped**. Fulfillment must see them in that order — shipping an unpaid order is a refund waiting to happen. Kafka promises order, but only inside one partition. Today you choose the key that decides the partition, prove the right key keeps every order in order, and then use a plausible wrong key and count the damage.

## What you build

- The `orders` topic now has **6 partitions**, and fulfillment-service reads it with **3 listener threads** (`concurrency = 3`), each owning two partitions.
- `OrderEvent` carries a `type` and a `seq` (1, 2, 3).
- `POST /orders/simulate {"orders": N, "keyMode": "orderId" | "eventId"}` publishes the three events of N orders back to back.
- `OrderTracker` checks, per order, that each event's `seq` is the last one plus one, and records which partitions an order's events used.

## The key is the partition

Kafka's default partitioner hashes a non-null key with murmur2 and takes it modulo the partition count. `Keys.partitionFor` uses the same formula, so you can predict where an order will land:

```java
public static String keyFor(OrderEvent e, String mode) {
    return "eventId".equals(mode) ? e.eventId() : e.orderId();
}

public static int partitionFor(String key, int partitions) {
    return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
}
```

Key by `orderId` and all three events of `ORD-1001` hash to the same partition. One partition is an append-only log read by one thread, so they are consumed in the order they were written.

Key by `eventId` — a unique id per event, a common mistake when someone wants "even spreading" — and each event of an order lands on its own partition. Three threads now race through different partitions. Nothing in Kafka is broken; you simply asked it to put one order's history in three places.

## Checking order on the consumer

```java
int last = lastSeq.getOrDefault(e.orderId(), 0);
boolean inOrder = e.seq() == last + 1;
if (!inOrder) {
    outOfOrder++;
    disordered.add(e.orderId());
}
```

An order counts as "shipped in order" only if all three events arrived 1, 2, 3.

## What we measured

300 orders, 900 events, 6 partitions, 3 listener threads, in the browser sandbox:

| Key | Out-of-order events | Orders shipped in order | Orders split across partitions |
|---|---|---|---|
| `orderId` | **0** of 900 | 300 / 300 | 0 |
| `eventId` | **520** of 900 (57.8%) | 81 / 300 | 293 |

Both runs spread the load across all six partitions; the wrong key did not buy better balance, only disorder. More than half the events arrived before the event they depend on — a Paid before its Placed, a Shipped before its Paid.

## When the right key is not enough

- **Hot keys.** All events for one key go to one partition. A customer with 50,000 orders keyed by customer id makes one partition (and one consumer thread) the bottleneck. Key by the entity whose order matters — here the order, not the customer.
- **Changing the partition count** changes `hash % partitions`, so existing keys move. Pick a partition count with headroom, and add partitions only when you can tolerate a short window of reordering.
- **Retries can reorder within a partition** unless the producer is idempotent (on by default in Kafka 3.x; Day 4 shows why it matters).

## Try it

```bash
curl -s localhost:8081/orders/ORD-1001/partition
curl -s -X POST localhost:8081/orders/simulate -H 'Content-Type: application/json' -d '{"orders":50,"keyMode":"eventId"}'
curl -s localhost:8082/stats
```

## Say it in an interview

**Q: Does Kafka guarantee message ordering?**
A: Only within a partition. Events with the same key go to the same partition and are read in write order by one consumer thread; across partitions there is no global order.

**Q: How do you choose a partition key?**
A: Use the entity whose events must stay ordered (the order id for an order lifecycle), check it has enough distinct values to spread load, and watch for hot keys. Don't use a unique-per-event id if order matters.

**Q: What happens to ordering if you add partitions?**
A: The key-to-partition mapping changes for many keys, so new events for an existing key may land on a different partition than its older events; consumers can briefly see them out of order.

## Recap

- Same key, same partition, same order. Different partitions, no order.
- `murmur2(key) % partitions` decides placement; you can compute it yourself.
- Measured: key by order id, 0 of 900 out of order; key by event id, 520 of 900 (57.8%), 293 of 300 orders split.
- The wrong key gave no better balance — only disorder.
- Watch for hot keys and for partition-count changes.

## Challenge (10 minutes)

Key by `customerId` instead. Is order per order preserved? What does the per-partition distribution look like when one customer places a third of all orders?

<details class="sdc-hint"><summary>Hint 1</summary>Add a `"customerId"` branch to `Keys.keyFor` and pass `keyMode: "customerId"` to `/orders/simulate`.</details>

<details class="sdc-hint"><summary>Hint 2</summary>All of one customer's orders share a partition, so each order stays in order — but `eventsByPartition` in `/stats` shows the skew.</details>

<details class="sdc-hint"><summary>Hint 3</summary>Make customer `C-0` place every third order (change the `"C-" + (i % 50)` expression) and compare the busiest partition's count with the others.</details>

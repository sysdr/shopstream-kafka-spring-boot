order-service now does two things for every order: it saves the order in its database, and it publishes `OrderPlaced` to Kafka. Two systems, two writes, no shared transaction. If the process dies between them, the database says the order exists and Kafka never hears about it: the customer is charged, the warehouse never ships. Today you make that crash happen on purpose, count the orders that fall through the gap, and close the gap with the **transactional outbox**.

## What you build

- **A database for order-service**: H2 in a file (`.run/orders-db`), with tables `orders` and `outbox` and a sequence for order numbers. `WRITE_DELAY=0` makes every commit durable immediately — without it, a crash a few hundred milliseconds after COMMIT can still undo it, which our first run showed.
- **Two ways to place an order** (`POST /orders/batch {"mode": "dual"|"outbox", "count": N}`):
  - `dual`: commit the order, then `kafka.send().get()`.
  - `outbox`: insert the order **and** an outbox row holding the event, in one transaction. Nothing is sent to Kafka here.
- **`OutboxRelay`**, a `@Scheduled` job every 100 ms: read unpublished outbox rows in id order, publish each, then set `published_at`.
- **`CrashSwitch`**: with `--app.crash.every-n=100` the JVM halts (`Runtime.halt`, like `kill -9`) at every 100th crash point — right after COMMIT in dual mode, right after the send and before the mark in the relay.

## The gap, in code

```java
public String placeDualWrite(int i) throws Exception {
    OrderEvent e = newOrder("DUAL", i);
    tx.executeWithoutResult(s -> insertOrder(e));               // write 1: committed
    crash.maybe("after the database commit, before the Kafka publish");
    kafka.send(Topics.ORDERS, e.orderId(), json.writeValueAsString(e)).get(10, TimeUnit.SECONDS); // write 2
    return e.orderId();
}
```

Reordering the writes does not help: publish first and a crash before COMMIT announces an order that does not exist. Wrapping both in `@Transactional` does not help either — Kafka is not part of the database transaction.

## The outbox, in code

```java
// one transaction: both rows or neither
insertOrder(e);
db.update("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (?, ?, ?)",
        e.orderId(), e.type(), json.writeValueAsString(e));
```

```java
@Scheduled(fixedDelay = 100)
public void relay() throws Exception {
    for (Map<String, Object> row : unpublishedRowsInOrder()) {
        kafka.send(Topics.ORDERS, key, payload).get(10, TimeUnit.SECONDS);
        crash.maybe("after publishing outbox row, before marking it published");
        db.update("UPDATE outbox SET published_at = CURRENT_TIMESTAMP WHERE id = ?", id);
    }
}
```

Now there is only one write that can fail independently — the publish — and it is retried until it succeeds, because the row stays unpublished until the mark. The cost: a crash after the send and before the mark publishes that row **again** on restart. The outbox gives at-least-once delivery, so the consumer must be idempotent. Fulfillment already de-duplicates by order id (`RunTracker`), so a re-delivered order is fulfilled once.

## What we measured

300 orders per strategy, a crash on every 100th crash point, order-service restarted after each crash, in the browser sandbox:

| Strategy | Crashes | Orders in the database | Delivered to fulfillment | **Mismatched** | Delivered twice |
|---|---|---|---|---|---|
| Dual write | 3 | 300 | 297 | **3** | 0 |
| Outbox | 3 | 300 | 300 | **0** | 3 |

Every dual-write crash cost exactly one order: `DUAL-000150`, `DUAL-000278` and `DUAL-000406` exist in the database, and no event for them will ever be published. Nothing retries them, nothing reports them; you would find them weeks later in a reconciliation report. Every outbox crash cost one duplicate instead — visible, harmless, and absorbed by the consumer.

## Things to know before you ship it

- **Ordering:** the relay reads rows in id order and publishes with the order id as key, so per-order ordering from Day 2 holds.
- **Throughput:** polling every 100 ms adds up to 100 ms of latency. Change-data-capture (Debezium reading the database log) removes the polling and the extra query load.
- **Cleanup:** delete published rows after a while, or the outbox table grows forever.
- **Several relay instances** need to avoid publishing the same rows: `SELECT ... FOR UPDATE SKIP LOCKED`, or a single active relay.

## Try it

```bash
curl -s -X POST localhost:8081/orders/batch -H 'Content-Type: application/json' -d '{"mode":"outbox","count":10}'
curl -s localhost:8081/outbox
curl -s 'localhost:8082/runs/OBX'
```

## Say it in an interview

**Q: What is the dual-write problem?**
A: A service writes to its database and to a message broker in two separate operations; a crash between them leaves the two out of sync, and no single transaction can cover both.

**Q: How does the transactional outbox solve it?**
A: The event is written to an outbox table in the same database transaction as the business change; a relay (polling or CDC) publishes outbox rows afterwards and marks them sent. The database becomes the single source of truth for "this happened".

**Q: What delivery guarantee does the outbox give, and what does that require downstream?**
A: At-least-once: a crash after publishing and before marking re-sends the row. Consumers must be idempotent, for example by de-duplicating on the event or order id.

## Recap

- Two writes to two systems cannot be made atomic with a local transaction.
- Measured: dual write with 3 crashes left 3 orders saved but never announced; the outbox with 3 crashes left 0, at the cost of 3 duplicate deliveries.
- The outbox turns "maybe lost" into "maybe duplicated"; idempotent consumers make duplicates harmless.
- Commit durability matters: with H2's default write delay, a crash could undo recent commits.

## Challenge (10 minutes)

Make fulfillment truly idempotent across restarts: today it de-duplicates in memory, so a duplicate that arrives after a fulfillment restart would be processed again.

<details class="sdc-hint"><summary>Hint 1</summary>Give fulfillment its own table, `processed_orders(order_id PRIMARY KEY)`, and insert into it before doing the work.</details>

<details class="sdc-hint"><summary>Hint 2</summary>A duplicate key exception on that insert means "already processed": log it and return without doing the work.</details>

<details class="sdc-hint"><summary>Hint 3</summary>Test it by restarting fulfillment between the outbox crashes in the demo and checking that `duplicates` still never turns into double work.</details>

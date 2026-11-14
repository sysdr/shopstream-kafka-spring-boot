ShopStream's order-service and fulfillment-service talk over HTTP today. When fulfillment is slow, checkout is slow. When fulfillment is down, checkout fails. Today you put Kafka between them: order-service writes an **OrderPlaced** event to a topic and returns; fulfillment-service reads it whenever it can. You will measure how long an event takes to cross, then switch the consumer off and watch the events wait for it.

## What you build

- **A Kafka broker** in KRaft mode (no ZooKeeper, no Docker). `start.sh` downloads Kafka 3.9.1 once into `~/.cache/shopstream` and starts it on `localhost:19092`.
- **One Spring Boot jar, two services.** The `orders` profile runs order-service on port 8081; the `fulfillment` profile runs fulfillment-service on 8082. Same code base, different roles — the way many teams ship a small system.
- **A topic called `orders`** with 3 partitions, created by Spring's `KafkaAdmin` at start-up.

## The producer: publish and wait for the ack

`OrderController` turns the HTTP request into an event and sends it with the order id as the key:

```java
SendResult<String, String> sent = kafka.send(Topics.ORDERS, orderId, json.writeValueAsString(event))
        .get(10, TimeUnit.SECONDS);
```

`send()` is asynchronous; the `.get()` waits for the broker's acknowledgement. The response includes the partition and offset Kafka assigned, so the customer is only told "placed" once the event is stored. That is a deliberate trade: each request now costs one round-trip to Kafka. Day 4 looks at what "stored" really means.

The event carries `createdAtMs`, the producer's clock. That single field lets the consumer measure end-to-end latency without any tracing system.

## The consumer: a listener, not an endpoint

```java
@KafkaListener(topics = Topics.ORDERS, groupId = "fulfillment")
public void onOrderPlaced(String value) throws Exception {
    OrderPlaced event = json.readValue(value, OrderPlaced.class);
    stats.record(event.orderId(), System.currentTimeMillis() - event.createdAtMs());
}
```

Nobody calls fulfillment-service. Spring Kafka runs a poll loop: fetch a batch of records, call the method for each, then commit the **offset** (the position in the partition) for the group `fulfillment`. The offset is how Kafka remembers where this consumer group stopped.

## What we measured

`start.sh` builds the jar, runs the unit tests, starts both services and the demo. In the browser sandbox (Ubuntu 24.04, Java 21):

| Measurement | Result |
|---|---|
| 200 orders through order-service | 4.17 s, 48 orders/s (each request waits for the ack) |
| End-to-end latency, producer to consumer | p50 6 ms, p99 80 ms, max 83 ms |
| Orders accepted while the consumer was stopped | 50 |
| Consumer-group lag while stopped | 50 (every event waiting in Kafka) |
| Catch-up after restart | 5.7 s, including JVM start-up |
| Events lost | 0 (the oldest waited 9.5 s) |

Two things stand out. First, the slow part of the HTTP path is the synchronous ack, not Kafka itself: events reach the consumer in single-digit milliseconds. Second, stopping the consumer did not stop checkout. The 50 orders sat in the topic, the **lag** (log-end offset minus committed offset) said exactly how many, and the restarted consumer resumed from its committed offset.

## Why this matters in production

With HTTP, the caller and the callee must both be up at the same moment. With a log in the middle, they only need to be up eventually. That decoupling is what lets you deploy fulfillment at 3 p.m. on a busy day, or survive it crashing for ten minutes, without losing orders.

It also changes how you monitor. "Is fulfillment up?" becomes "how far behind is fulfillment?" — consumer lag is the metric you will alert on in Day 6.

## Try it

```bash
curl -s -X POST localhost:8081/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"C-1","sku":"SKU-7","quantity":2,"price":19.99}'
curl -s localhost:8082/stats
$HOME/.cache/shopstream/kafka_2.13-3.9.1/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:19092 --describe --group fulfillment
```

## Say it in an interview

**Q: Why put a message broker between two services instead of calling over HTTP?**
A: Temporal decoupling. The producer only needs the broker to be up; the consumer can be slow, deploying or down and catch up later from its committed offset. You trade a synchronous answer for durability and independent scaling.

**Q: What is consumer lag and why watch it?**
A: Lag is the log-end offset minus the group's committed offset, per partition. It is the number of events written but not yet processed, so it measures how far behind a consumer is — the right health signal for an asynchronous service.

**Q: The producer waits for an ack on every request. What does that cost and when would you not do it?**
A: One broker round-trip per request (48 orders/s single-threaded here). For fire-and-forget telemetry you send asynchronously and batch; for orders you want to know the event is stored before telling the customer.

## Recap

- Kafka decouples order-service from fulfillment-service in time: the consumer can be down and nothing is lost.
- `send().get()` waits for the broker ack; the response carries partition and offset.
- `@KafkaListener` runs a poll loop and commits offsets for its group.
- Measured: p50 6 ms, p99 80 ms end to end; 50 events waited while the consumer was stopped and arrived 5.7 s after restart.
- Lag, not "up or down", is how you judge an event consumer.

## Challenge (10 minutes)

Add a second consumer group, `billing`, that counts total revenue from the same `orders` topic. Stop fulfillment-service and show that billing keeps counting while fulfillment's lag grows.

<details class="sdc-hint"><summary>Hint 1</summary>A second `@KafkaListener(topics = Topics.ORDERS, groupId = "billing")` method receives every event too: groups do not share events, members of one group do.</details>

<details class="sdc-hint"><summary>Hint 2</summary>Keep the sum in an `AtomicLong` of cents to avoid floating-point drift, and expose it with a `@GetMapping("/billing")`.</details>

<details class="sdc-hint"><summary>Hint 3</summary>`kafka-consumer-groups.sh --describe --group billing` should show lag 0 while `--group fulfillment` shows the stopped consumer's backlog.</details>

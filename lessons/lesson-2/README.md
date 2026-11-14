# Implementation guide — Day 2: Keys, partitions and order

Time: 30–40 minutes. Needs Java 17+, Maven 3.8+, curl and Python 3 (the demo and checks use only its standard library). No Docker.

## Get the code

Open the browser sandbox from this page (everything is installed there), download **start.sh** and **stop.sh** from the Scripts tab, or clone the course repo:

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-2
```

`start.sh` writes the complete project for Day 2 into `./shopstream-events`, so you can start at any lesson.

## What start.sh does

1. Checks Java, Maven, curl and Python.
2. Downloads Kafka 3.9.1 once into `~/.cache/shopstream` (about 120 MB; later runs reuse it).
3. Starts a fresh KRaft cluster: 1 broker on localhost:19092.
4. Writes the project and runs `mvn package` (8 unit tests).
5. Starts order-service on 8081 and fulfillment-service on 8082.
6. Writes `verify.sh`, runs it, then runs the demo.

## What's new in the code

| File | What it does |
|---|---|
| `OrderEvent.java` | Replaces OrderPlaced: type and seq (Placed 1, Paid 2, Shipped 3) |
| `Keys.java` | keyFor(event, mode) and partitionFor(key) with Kafka's murmur2 formula |
| `OrderController.java` | POST /orders/simulate {orders, keyMode}; GET /orders/{id}/partition |
| `OrderTracker.java` | Per-order sequence check, partitions per order, events per partition |
| `FulfillmentService.java` | 3 listener threads (concurrency=3) over 6 partitions |

## Run it

```bash
bash start.sh
```

## Expected output

Timings depend on your machine; the counts should look like this (sandbox run):

```text
[VERIFY] 100 orders, 300 events keyed by order id: out-of-order 0, split orders 0
PASS  Lesson 2 complete. Your completion code: SHOP2-KEYS-3M7W
key = orderId: published 900 events for 300 orders in 793 ms
  consumed 900  out-of-order 0  orders shipped in order 300/300  orders split across partitions 0
key = eventId: published 900 events for 300 orders in 264 ms
  consumed 900  out-of-order 520  orders shipped in order 81/300  orders split across partitions 293
Wrong key: 520 of 900 events (57.8%) arrived before the event they depend on; right key: 0.
```

## Try it yourself

```bash
curl -s localhost:8081/orders/ORD-1001/partition
```

```bash
curl -s -X POST localhost:8081/orders/simulate -H 'Content-Type: application/json' -d '{"orders":50,"keyMode":"eventId"}'
```

```bash
curl -s localhost:8082/stats
```

## Verify and get your completion code

```bash
bash verify.sh
```

Publishes 100 orders keyed by order id and checks: 0 out-of-order events, no order split across partitions, at least 3 partitions used. On success it prints:

```text
PASS  Lesson 2 complete. Your completion code: SHOP2-KEYS-3M7W
```

Enter the code below the lesson to mark it complete.

## Stop

```bash
bash stop.sh
```

Stops both services and the Kafka brokers. The next `start.sh` begins with a fresh cluster.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Address already in use` | `bash stop.sh`; or set `ORDERS_PORT` / `FULFILLMENT_PORT` |
| `Kafka did not start` | Read `shopstream-events/.run/kafka-1/logs/console.log` |
| `build failed` | `cd shopstream-events && mvn package` shows the failing test |
| A service did not start | Read `shopstream-events/.run/orders.log` or `f1.log` |
| Kafka download fails | Download kafka_2.13-3.9.1.tgz yourself and set `KAFKA_HOME` to the extracted folder |

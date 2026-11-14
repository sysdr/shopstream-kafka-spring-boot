# Implementation guide — Day 1: Publish your first OrderPlaced event

Time: 25–35 minutes. Needs Java 17+, Maven 3.8+, curl and Python 3 (the demo and checks use only its standard library). No Docker.

## Get the code

Open the browser sandbox from this page (everything is installed there), download **start.sh** and **stop.sh** from the Scripts tab, or clone the course repo:

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-1
```

`start.sh` writes the complete project for Day 1 into `./shopstream-events`, so you can start at any lesson.

## What start.sh does

1. Checks Java, Maven, curl and Python.
2. Downloads Kafka 3.9.1 once into `~/.cache/shopstream` (about 120 MB; later runs reuse it).
3. Starts a fresh KRaft cluster: 1 broker on localhost:19092.
4. Writes the project and runs `mvn package` (4 unit tests).
5. Starts order-service on 8081 and fulfillment-service on 8082.
6. Writes `verify.sh`, runs it, then runs the demo.

## What's new in the code

| File | What it does |
|---|---|
| `pom.xml` | Spring Boot 3.5 parent, spring-boot-starter-web, spring-kafka |
| `ShopstreamEventsApplication.java` | One jar; the profile picks the role (orders or fulfillment) |
| `OrderPlaced.java` | The event (record) with the producer timestamp |
| `Topics.java` | Creates the orders topic (3 partitions) at start-up |
| `OrderController.java` | POST /orders: publish, wait for the ack, return partition and offset |
| `FulfillmentService.java` | @KafkaListener; records end-to-end latency; GET /stats |
| `tools/demo.py, tools/checks.py` | The measured demo and the checks verify.sh runs |

## Run it

```bash
bash start.sh
```

## Expected output

Timings depend on your machine; the counts should look like this (sandbox run):

```text
[VERIFY] both services healthy
[VERIFY] 20 events published and consumed; p99 end-to-end 77 ms
PASS  Lesson 1 complete. Your completion code: SHOP2-PUBLISH-5R8K
  published 200 in 4.17 s (48 orders/s, each waits for the broker ack)
  fulfillment received 200  end-to-end p50 6 ms  p99 80 ms  max 83 ms
  50 orders accepted while the consumer was down; consumer-group lag = 50 (events waiting in Kafka)
  caught up: received 50 waiting events 5.7 s after restart (includes JVM start-up)
  lag now = 0; nothing was lost, the oldest event waited 9.5 s
```

## Try it yourself

```bash
curl -s -X POST localhost:8081/orders -H 'Content-Type: application/json' -d '{"customerId":"C-1","sku":"SKU-7","quantity":2,"price":19.99}'
```

```bash
curl -s localhost:8082/stats
```

```bash
$HOME/.cache/shopstream/kafka_2.13-3.9.1/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group fulfillment
```

## Verify and get your completion code

```bash
bash verify.sh
```

Checks both services are healthy, sends one warm-up order (the first event also waits for the consumer to join its group), then publishes 20 orders and checks fulfillment received all 20 with p99 under 2 s. On success it prints:

```text
PASS  Lesson 1 complete. Your completion code: SHOP2-PUBLISH-5R8K
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

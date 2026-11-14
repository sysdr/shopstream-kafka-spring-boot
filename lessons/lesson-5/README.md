# Implementation guide — Day 5: Keep the database and events in step

Time: 35–45 minutes. Needs Java 17+, Maven 3.8+, curl and Python 3 (the demo and checks use only its standard library). No Docker.

## Get the code

Open the browser sandbox from this page (everything is installed there), download **start.sh** and **stop.sh** from the Scripts tab, or clone the course repo:

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-5
```

`start.sh` writes the complete project for Day 5 into `./shopstream-events`, so you can start at any lesson.

## What start.sh does

1. Checks Java, Maven, curl and Python.
2. Downloads Kafka 3.9.1 once into `~/.cache/shopstream` (about 120 MB; later runs reuse it).
3. Starts a fresh KRaft cluster: 1 broker on localhost:19092.
4. Writes the project and runs `mvn package` (16 unit tests).
5. Starts order-service on 8081 and fulfillment-service on 8082.
6. Writes `verify.sh`, runs it, then runs the demo.

## What's new in the code

| File | What it does |
|---|---|
| `pom.xml` | Adds spring-boot-starter-jdbc and H2 |
| `schema.sql` | orders and outbox tables, order_seq |
| `application-orders.properties` | H2 file database; WRITE_DELAY=0 so a commit survives a crash |
| `OrderStore.java` | placeDualWrite (commit, then publish) and placeWithOutbox (one transaction) |
| `OutboxRelay.java` | @Scheduled every 100 ms: publish unpublished rows, then mark them |
| `CrashSwitch.java` | app.crash.every-n=N halts the JVM at the Nth crash point |
| `OutboxController.java` | POST /orders/batch {mode, count}; GET /orders/ids; GET /outbox |

## Run it

```bash
bash start.sh
```

## Expected output

Timings depend on your machine; the counts should look like this (sandbox run):

```text
[VERIFY] outbox: 50 new orders committed with their events, relay published all, missing at the consumer: 0
PASS  Lesson 5 complete. Your completion code: SHOP2-OUTBOX-2H9C
1) Dual write: 300 orders, order-service crashes on every 100th order, right after its COMMIT
   orders in the database 300   OrderPlaced events delivered 297
   MISMATCHED: 3 orders were saved but never announced -> ['DUAL-000150', 'DUAL-000278', 'DUAL-000406']
2) Outbox: 300 orders, the relay crashes on every 100th publish (after send, before marking it sent)
   relay crashes 3   orders in the database 300   distinct orders delivered 300
   MISMATCHED: 0   delivered twice after a crash: 3
```

## Try it yourself

```bash
curl -s -X POST localhost:8081/orders/batch -H 'Content-Type: application/json' -d '{"mode":"outbox","count":10}'
```

```bash
curl -s localhost:8081/outbox
```

```bash
curl -s 'localhost:8082/runs/OBX'
```

## Verify and get your completion code

```bash
bash verify.sh
```

Places 50 orders through the outbox and checks the relay published all of them and every committed order reached the consumer. On success it prints:

```text
PASS  Lesson 5 complete. Your completion code: SHOP2-OUTBOX-2H9C
```

Enter the code below the lesson to mark it complete.

## Stop

```bash
bash stop.sh
```

Stops both services and the Kafka brokers. The next `start.sh` begins with a fresh cluster and database.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Address already in use` | `bash stop.sh`; or set `ORDERS_PORT` / `FULFILLMENT_PORT` |
| `Kafka did not start` | Read `shopstream-events/.run/kafka-1/logs/console.log` |
| `build failed` | `cd shopstream-events && mvn package` shows the failing test |
| A service did not start | Read `shopstream-events/.run/orders.log` or `f1.log` |
| Kafka download fails | Download kafka_2.13-3.9.1.tgz yourself and set `KAFKA_HOME` to the extracted folder |

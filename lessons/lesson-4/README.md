# Implementation guide — Day 4: Never lose an order

Time: 35–45 minutes. Needs Java 17+, Maven 3.8+, curl and Python 3 (the demo and checks use only its standard library). No Docker.

## Get the code

Open the browser sandbox from this page (everything is installed there), download **start.sh** and **stop.sh** from the Scripts tab, or clone the course repo:

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-4
```

`start.sh` writes the complete project for Day 4 into `./shopstream-events`, so you can start at any lesson.

## What start.sh does

1. Checks Java, Maven, curl and Python.
2. Downloads Kafka 3.9.1 once into `~/.cache/shopstream` (about 120 MB; later runs reuse it).
3. Starts a fresh KRaft cluster: 3 brokers on localhost:19092, 19192, 19292.
4. Writes the project and runs `mvn package` (13 unit tests).
5. Starts order-service on 8081 and fulfillment-service on 8082.
6. Writes `verify.sh`, runs it, then runs the demo.

## What's new in the code

| File | What it does |
|---|---|
| `Topics.java` | orders: 3 replicas, min.insync.replicas=2 |
| `ProducerModes.java` | Three producers: acks1, acksAll, safe (acks=all + idempotence) |
| `BurstService.java` | POST /bursts: paced burst, remembers every acknowledged order id |
| `ClusterController.java` | GET /cluster: brokers, partition leaders, under-replicated partitions |
| `RunTracker.java` | Consumer side: received and distinct orders per run |

## Run it

```bash
bash start.sh
```

## Expected output

Timings depend on your machine; the counts should look like this (sandbox run):

```text
[VERIFY] brokers [1, 2, 3]  replicas per partition 3  under-replicated 0
[VERIFY] safe producer: acknowledged 2000/2000, consumed 2000 distinct, duplicates 0
PASS  Lesson 4 complete. Your completion code: SHOP2-ACKS-6Q4Z
mode      acked   failed   lost   duplicates
acks1     20000       0     65          0
acksAll   20000       0      0        449
safe      20000       0      0          0
```

## Try it yourself

```bash
curl -s localhost:8081/cluster
```

```bash
curl -s -X POST localhost:8081/bursts -H 'Content-Type: application/json' -d '{"runId":"mine","mode":"acks1","count":5000,"ratePerSec":2000}'
```

```bash
kill -9 $(cat shopstream-events/.run/kafka-1.pid); sleep 5; curl -s localhost:8081/bursts/mine; curl -s localhost:8082/runs/mine
```

```bash
bash shopstream-events/.run/kafka-1/start-broker.sh
```

## Verify and get your completion code

```bash
bash verify.sh
```

Checks 3 brokers and 3 replicas per partition, then sends 2,000 orders with the safe producer and checks all 2,000 arrive exactly once. On success it prints:

```text
PASS  Lesson 4 complete. Your completion code: SHOP2-ACKS-6Q4Z
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

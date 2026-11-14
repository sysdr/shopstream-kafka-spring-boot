# Implementation guide — Day 3: Scale consumers without chaos

Time: 35–45 minutes. Needs Java 17+, Maven 3.8+, curl and Python 3 (the demo and checks use only its standard library). No Docker.

## Get the code

Open the browser sandbox from this page (everything is installed there), download **start.sh** and **stop.sh** from the Scripts tab, or clone the course repo:

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-3
```

`start.sh` writes the complete project for Day 3 into `./shopstream-events`, so you can start at any lesson.

## What start.sh does

1. Checks Java, Maven, curl and Python.
2. Downloads Kafka 3.9.1 once into `~/.cache/shopstream` (about 120 MB; later runs reuse it).
3. Starts a fresh KRaft cluster: 1 broker on localhost:19092.
4. Writes the project and runs `mvn package` (9 unit tests).
5. Starts order-service on 8081 and fulfillment-service on 8082 (plus f2 on 8083 and f3 on 8084).
6. Writes `verify.sh`, runs it, then runs the demo.

## What's new in the code

| File | What it does |
|---|---|
| `RebalanceTracker.java` | Counts rebalances and the partitions this instance owns |
| `ListenerConfig.java` | Attaches the tracker to the orders listener container |
| `LagService.java` | Lag per partition with the Kafka AdminClient |
| `FulfillmentService.java` | GET /lag; app.instance names f1, f2, f3 |
| `application.properties` | CooperativeStickyAssignor |

## Run it

```bash
bash start.sh
```

## Expected output

Timings depend on your machine; the counts should look like this (sandbox run):

```text
[VERIFY] partitions owned: f1=[0, 3], f2=[2, 5], f3=[1, 4]
[VERIFY] 300 events consumed by the group exactly once (counted 300), lag 0
PASS  Lesson 3 complete. Your completion code: SHOP2-GROUP-8T2D
  1 consumer : 900 events  drained in 10.1 s  peak lag 890  rebalances 0  re-processed 0
  3 consumers: 900 events  drained in 4.2 s  peak lag 895  rebalances 0  re-processed 0
  storm      : 300 events  drained in not drained after 45 s (lag 256)  peak lag 300  rebalances 9  re-processed 715
  fixed      : 300 events  drained in 12.1 s  peak lag 300  rebalances 0  re-processed 0
```

## Try it yourself

```bash
for p in 8082 8083 8084; do curl -s localhost:$p/stats; echo; done
```

```bash
curl -s localhost:8082/lag
```

```bash
kill $(cat shopstream-events/.run/f2.pid); sleep 5; curl -s localhost:8082/stats   # f1 and f3 take over f2's partitions
```

## Verify and get your completion code

```bash
bash verify.sh
```

Checks the 6 partitions are split across the 3 instances with no overlap, then publishes 300 events and checks the group processed each exactly once and lag is 0. On success it prints:

```text
PASS  Lesson 3 complete. Your completion code: SHOP2-GROUP-8T2D
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

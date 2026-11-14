# Implementation guide — Day 6: Run it like production

Time: 40–50 minutes. Needs Java 17+, Maven 3.8+, curl and Python 3 (the demo and checks use only its standard library). No Docker.

## Get the code

Open the browser sandbox from this page (everything is installed there), download **start.sh** and **stop.sh** from the Scripts tab, or clone the course repo:

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-6
```

`start.sh` writes the complete project for Day 6 into `./shopstream-events`, so you can start at any lesson.

## What start.sh does

1. Checks Java, Maven, curl and Python.
2. Downloads Kafka 3.9.1 once into `~/.cache/shopstream` (about 120 MB; later runs reuse it).
3. Starts a fresh KRaft cluster: 3 brokers on localhost:19092, 19192, 19292.
4. Writes the project and runs `mvn package` (18 unit tests).
5. Starts order-service on 8081 and fulfillment-service on 8082.
6. Writes `verify.sh`, runs it, then runs the demo.

## What's new in the code

| File | What it does |
|---|---|
| `Topics.java` | orders: 7-day retention; orders.DLT: 30-day retention |
| `DeadLetterConfig.java` | DefaultErrorHandler: 2 retries, JSON errors not retried, then orders.DLT |
| `DeadLetterInspector.java` | Reads orders.DLT; GET /ops/dlt with the reason and original offset |
| `LagAlert.java` | OK / PENDING / FIRING with a 5 s 'for' duration; GET /ops/alert |
| `OpsController.java` | GET /ops/topic; POST /ops/poison |
| `BurstService.java` | Also records the longest gap between acknowledgements |

## Run it

```bash
bash start.sh
```

## Expected output

Timings depend on your machine; the counts should look like this (sandbox run):

```text
[VERIFY] orders topic: retention 7 days, min.insync.replicas 2, 3 brokers
[VERIFY] 3 poison messages in orders.DLT, 50 good orders still processed, alert state OK
PASS  Lesson 6 complete. Your completion code: SHOP2-OPS-7L3V
   good orders processed 300/300   dead letters 5   consumer lag 0
   alert FIRING 8.5 s after the consumer stopped (lag 3000)
   consumer restarted; alert back to OK 1.5 s later
   producer: acknowledged 4000/4000  failed 0  longest stall 8605 ms
   consumer: peak lag 20  back under 100 11.4 s after the kill
   broker 3 restarted, all replicas in sync 37.8 s after the kill
   lost 0   duplicates 0
```

## Try it yourself

```bash
curl -s localhost:8081/ops/topic
```

```bash
curl -s -X POST 'localhost:8081/ops/poison?count=2'; sleep 2; curl -s localhost:8082/ops/dlt
```

```bash
curl -s localhost:8081/ops/alert
```

## Verify and get your completion code

```bash
bash verify.sh
```

Checks the retention and min.insync.replicas settings, that the lag alert is running, that 3 poison messages land in orders.DLT and that 50 good orders still get through. On success it prints:

```text
PASS  Lesson 6 complete. Your completion code: SHOP2-OPS-7L3V
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

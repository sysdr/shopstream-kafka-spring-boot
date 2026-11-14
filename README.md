# Kafka with Spring Boot: Make ShopStream Event-Driven

Source code for the free course **[Kafka with Spring Boot: Make ShopStream Event-Driven](https://systemdrd.com/courses/kafka-with-spring-boot-make-shopstream-event-driven/)** on systemdrd.com — step 2 of the free ShopStream path.

ShopStream's order-service and fulfillment-service stop calling each other and start talking through Kafka. Over six lessons you publish events, keep them in order, scale consumers, survive a broker crash without losing or duplicating an order, keep the database and the events in step with the outbox pattern, and add the operational pieces production needs.

- **Java 17+, Maven 3.8+, Spring Boot 3.5, Kafka 3.9 (KRaft).** No Docker: `start.sh` downloads Kafka once into `~/.cache/shopstream` and runs it locally.
- **Python 3** (standard library only) drives the demos and checks.
- Every lesson runs in the browser sandbox from its lesson page; nothing to install.

## Lessons

Each lesson folder is self-contained: `start.sh` starts a fresh Kafka cluster, writes the complete project up to that day into `./shopstream-events`, builds and tests it, starts the services, verifies them and runs a measured demo. `README.md` is the implementation guide, `NOTES.md` the lesson notes, `diagrams/` the diagrams.

| Day | Lesson | What you measure |
|---|---|---|
| 1 | [Publish your first OrderPlaced event](lessons/lesson-1) | p50 6 ms / p99 80 ms end to end; 50 events wait while the consumer is down, none lost |
| 2 | [Keys, partitions and order](lessons/lesson-2) | key by order id: 0 of 900 out of order; key by event id: 520 of 900 |
| 3 | [Scale consumers without chaos](lessons/lesson-3) | 1 → 3 consumers: 10.1 s → 4.2 s; a rebalance storm: 9 rebalances, 715 re-processed |
| 4 | [Never lose an order](lessons/lesson-4) | leader killed mid-burst: acks=1 lost 65, acks=all without idempotence 449 duplicates, safe 0/0 |
| 5 | [Keep the database and events in step](lessons/lesson-5) | dual write: 3 orders never announced; outbox: 0 |
| 6 | [Run it like production](lessons/lesson-6) | dead-letter topic, lag alert, broker drill: 0 lost, 8.6 s stall |

## Quick start

```bash
git clone https://github.com/sysdr/shopstream-kafka-spring-boot.git
cd shopstream-kafka-spring-boot/lessons/lesson-1
bash start.sh     # Kafka, build, tests, services, verify, demo
bash stop.sh
```

`bash test_all.sh` runs all six lessons (about 15 minutes).

## The finished project

The repository root holds the final code (Day 6): `pom.xml`, `src/` and `tools/`. `mvn package` builds `target/shopstream-events.jar` and runs the 18 unit tests. To run the whole system with its 3-broker cluster, use `lessons/lesson-6/start.sh`.

| Package `com.shopstream.events` | Role |
|---|---|
| `OrderController`, `Keys` | publish order events, keyed by order id |
| `FulfillmentService`, `OrderTracker` | consume, check per-order sequence, latency |
| `RebalanceTracker`, `LagService` | group membership and lag |
| `ProducerModes`, `BurstService`, `ClusterController` | acks / idempotence experiments, cluster state |
| `OrderStore`, `OutboxRelay`, `CrashSwitch` | transactional outbox and crash injection |
| `DeadLetterConfig`, `DeadLetterInspector`, `LagAlert`, `OpsController` | operations |

## License

MIT

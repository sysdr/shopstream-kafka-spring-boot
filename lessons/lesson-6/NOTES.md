The code works. Now it has to survive a Tuesday: a malformed message from a partner integration, a consumer that falls behind during a deploy, a broker that dies at 2 a.m. Today you add the operational pieces every Kafka system needs — retention, a dead-letter topic, a lag alert — and run a broker failure drill against the 3-broker cluster from Day 4 to measure how long recovery really takes.

## What you build

- **Retention** on the `orders` topic: 7 days (`retention.ms=604800000`), set in `Topics.java` and checked through the AdminClient at `GET /ops/topic`.
- **A dead-letter topic**, `orders.DLT` (30-day retention). Spring Kafka's `DefaultErrorHandler` retries a failing record twice, 200 ms apart, then publishes it to `orders.DLT` with the exception in its headers and moves on. A record that cannot even be parsed is not retried at all.
- **A lag alert** with a "for" duration, like a Prometheus rule: `OK` → `PENDING` when lag passes 500 → `FIRING` if it stays above 500 for 5 s → `OK` when it drops back.
- **A failure drill**: 200 orders/s for 20 s with the safe producer, `kill -9` the leader of partition 0 after 5 s, then measure the producer stall, the consumer lag and the time until all replicas are back in sync.

## The dead-letter topic

```java
DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
        (rec, ex) -> new TopicPartition(Topics.ORDERS_DLT, rec.partition()));
DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2L));
handler.addNotRetryableExceptions(JsonProcessingException.class);
```

Without this, a single unreadable record ("poison pill") is retried forever and its partition stops moving — every order behind it waits. With it, the bad record is parked with its original partition, offset and exception class, and the consumer keeps going. Note the explicit destination: Spring Kafka 3.3 would otherwise publish to `orders-dlt`, a topic this cluster does not auto-create. We found that the hard way in the first run.

## The alert

```java
if (lag <= threshold) { aboveSince = -1; next = "OK"; }
else {
    if (aboveSince < 0) aboveSince = nowMs;
    next = nowMs - aboveSince >= forMs ? "FIRING" : "PENDING";
}
```

The "for" duration is what makes an alert worth waking up for: a deploy that causes a 3-second spike never fires. The evaluator runs in order-service because that service stays up while fulfillment is down — in production this lives in Prometheus or your monitoring system, fed by the same lag numbers.

## What we measured

In the browser sandbox:

| Check | Result |
|---|---|
| Topic settings | `retention.ms` 7 days, `min.insync.replicas` 2, `cleanup.policy` delete |
| 5 poison messages mixed into 300 good orders | 300 / 300 good orders processed, 5 in `orders.DLT`, lag 0 |
| Dead letter example | key `POISON-3`, partition 2, offset 10, cause `JsonParseException` |
| Consumer stopped, 300 orders/s arriving | alert `PENDING` at lag 733, `FIRING` 8.5 s after the stop (lag 2,420) |
| Consumer restarted | alert back to `OK` 1.5 s later (lag 0) |
| Broker drill: leader of partition 0 killed | 4,000 / 4,000 acknowledged, 0 failed, **0 lost, 0 duplicates** |
| Longest producer stall | 8.6 s |
| Consumer lag | peak 20; under 100 again 11.4 s after the kill |
| Killed broker restarted, all replicas in sync | 37.8 s after the kill |

The interesting number is the **8.6 s stall**. Nothing failed, nothing was lost, but for about nine seconds the producer could not write to the partitions that had lost their leader. A `kill -9` sends no goodbye; the controller only declares the broker dead when its session times out (`broker.session.timeout.ms`, 9 s by default), then elects new leaders from the in-sync replicas. A clean shutdown (`kafka-server-stop.sh`) hands leadership over in well under a second — which is why rolling restarts should always be clean.

## A runbook in five lines

1. **Lag alert fires:** check the consumer is running and not crash-looping; check for a rebalance storm (Day 3).
2. **Dead letters appear:** read `/ops/dlt` (or the DLT topic) — the cause header says if it's bad data or a bug; fix, then replay the DLT into `orders`.
3. **Producer errors with `NotEnoughReplicas`:** two brokers are down; writes are refused on purpose (Day 4).
4. **Broker down:** restart it; watch under-replicated partitions return to 0.
5. **Retention:** a consumer down longer than 7 days loses data it never read — alert long before that.

## Try it

```bash
curl -s localhost:8081/ops/topic
curl -s -X POST 'localhost:8081/ops/poison?count=2'; sleep 2; curl -s localhost:8082/ops/dlt
curl -s localhost:8081/ops/alert
```

## Say it in an interview

**Q: What is a dead-letter topic and when do you use one?**
A: A topic where a consumer parks records it cannot process after a bounded number of retries, with the error in headers, so one bad record does not block its partition. Non-retryable errors such as parse failures go straight there.

**Q: How do you alert on a Kafka consumer?**
A: On consumer-group lag, with a threshold and a "for" duration so short spikes don't page anyone; also on the lag trend and on the consumer being absent from its group.

**Q: What happens to producers when a broker dies?**
A: Partitions it led are unavailable until the controller notices (session timeout) and elects new leaders from the ISR; idempotent producers retry and continue. Measured here: an 8.6 s stall, 0 lost, 0 duplicated.

## Recap

- 7-day retention, a dead-letter topic with bounded retries, and a lag alert with a "for" duration are the minimum for production.
- Measured: 5 poison messages parked, 300/300 good orders processed; the alert fired 8.5 s after the consumer stopped and cleared 1.5 s after restart.
- A leader killed mid-traffic: 0 lost, 0 duplicates, an 8.6 s producer stall, full redundancy back after 37.8 s.
- `kill -9` costs a session timeout; clean shutdowns fail over almost instantly.

## Challenge (10 minutes)

Build a "replay" endpoint that moves records from `orders.DLT` back to `orders` once the bug is fixed — and make sure a record that fails again does not loop forever.

<details class="sdc-hint"><summary>Hint 1</summary>Use a `KafkaConsumer` (or `@KafkaListener` that you start on demand) on `orders.DLT` and republish each value with the same key.</details>

<details class="sdc-hint"><summary>Hint 2</summary>Add a header such as `replay-count` and send the record back to the DLT instead of replaying it when the count reaches 3.</details>

<details class="sdc-hint"><summary>Hint 3</summary>Poison records sent by `/ops/poison` will never parse, so they should end up in the DLT again with `replay-count=3` — check with `/ops/dlt`.</details>

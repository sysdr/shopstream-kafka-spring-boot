One fulfillment-service instance cannot keep up on a busy day. The obvious fix is to run three. Kafka makes that easy — every instance joins the same **consumer group** and the brokers split the partitions between them — and it also makes it easy to get wrong: a consumer that takes too long between polls is thrown out of the group, its partitions move, its work is redone, and the group spends its time rebalancing instead of fulfilling orders. Today you scale out, measure the gain, then cause a rebalance storm and fix it with one setting.

## What you build

- **Three fulfillment instances** (`f1`, `f2`, `f3` on ports 8082–8084) in the group `fulfillment`, one listener thread each, reading the 6-partition `orders` topic. Each event costs 10 ms of simulated work.
- **`RebalanceTracker`**, a `ConsumerAwareRebalanceListener` attached to the orders listener container: it counts rebalances and remembers which partitions this instance owns.
- **`LagService`**: lag per partition from the Kafka AdminClient (log-end offset minus committed offset), at `GET /lag`.
- **Cooperative rebalancing** (`CooperativeStickyAssignor`): when a member joins or leaves, only the partitions that have to move are revoked; the rest keep flowing.

## How the group shares the work

Each partition is owned by exactly one member of a group. With 6 partitions and 3 members, each owns 2; a fourth member would get some, a seventh would sit idle. **The partition count is the ceiling on parallelism** for one group — choose it with growth in mind (Day 2 explains why changing it later is not free).

When a member joins, leaves or stops polling, the group coordinator (a broker) runs a rebalance and hands out partitions again. Membership is proven by polling: if a consumer doesn't call `poll()` within `max.poll.interval.ms` (here 5 s), the coordinator assumes it is stuck and evicts it.

## The storm, in one line of arithmetic

```text
max.poll.records = 100, work per record = 100 ms  ->  one batch = 10 s  >  max.poll.interval.ms = 5 s
```

Each consumer fetches 100 records and starts working. After 5 s the coordinator evicts it and gives its partitions to someone else. When it finishes, its offset commit is rejected — it no longer owns those partitions — so the new owner processes the same records again. Then the evicted consumer polls, rejoins, triggers another rebalance... and the cycle repeats.

The fix is not "more consumers"; it is a batch that fits inside the interval:

```text
max.poll.records = 10  ->  one batch = 1 s  <  5 s
```

(Raising `max.poll.interval.ms` also works, but it makes a really stuck consumer take longer to detect.)

## What we measured

900 events (300 orders) for the scaling test, 300 events for the storm, in the browser sandbox:

| Setup | Drained in | Peak lag | Rebalances | Events processed twice |
|---|---|---|---|---|
| 1 consumer, 10 ms/event | 10.1 s | 890 | 0 | 0 |
| 3 consumers, 10 ms/event | **4.2 s** | 895 | 0 | 0 |
| 3 consumers, 100 ms/event, 100 records per poll | **not drained after 45 s** (lag 256) | 300 | **9** | **715** |
| 3 consumers, 100 ms/event, 10 records per poll | 12.1 s | 300 | 0 | 0 |

Three consumers drained 2.4× faster than one (not 3×: partitions are not perfectly even and each member has some fixed overhead). The storm is the striking row: with the same three consumers and the same work, the group processed 715 events twice and still had 256 left after 45 seconds. The ownership snapshot taken during the storm even showed two instances each believing they owned all six partitions — evicted members only learn they lost their partitions when they next poll. One setting — 10 records per poll instead of 100 — brought it back to 0 rebalances, 0 duplicates and a 12.1 s drain.

## Rules of thumb

- Size `max.poll.records × worst-case time per record` well under `max.poll.interval.ms`.
- Make processing idempotent: rebalances always mean some redelivery (Day 5).
- Use cooperative-sticky assignment so a deploy doesn't stop the whole group.
- Watch rebalance counts as well as lag; a steadily rising count is the storm's signature.

## Try it

```bash
for p in 8082 8083 8084; do curl -s localhost:$p/stats; echo; done
curl -s localhost:8082/lag
kill $(cat shopstream-events/.run/f2.pid); sleep 5
curl -s localhost:8082/stats    # f1 and f3 took over f2's partitions
```

## Say it in an interview

**Q: How do you scale a Kafka consumer?**
A: Run more members of the same consumer group; the brokers assign each partition to one member. Parallelism is capped by the partition count, so you size partitions for the throughput you expect.

**Q: What causes a rebalance storm?**
A: Members repeatedly leaving and rejoining — most often because processing a batch takes longer than `max.poll.interval.ms`, so the coordinator evicts the member, its uncommitted work is redone elsewhere, and it rejoins and triggers another rebalance.

**Q: Eager vs cooperative rebalancing?**
A: Eager revokes every partition from every member and reassigns from scratch, pausing the whole group. Cooperative (incremental) only moves the partitions that need to move, so the others keep processing.

## Recap

- A consumer group splits partitions among members; partitions cap parallelism.
- Measured: 1 → 3 consumers, 10.1 s → 4.2 s to drain 900 events.
- Slow batches beyond `max.poll.interval.ms` caused 9 rebalances and 715 duplicate deliveries, with work left undone after 45 s.
- 10 records per poll instead of 100: 0 rebalances, 0 duplicates, drained in 12.1 s.

## Challenge (10 minutes)

Start a fourth instance, `f4`, on port 8085. How many partitions does it get? Then start a seventh member — what does it do?

<details class="sdc-hint"><summary>Hint 1</summary>In `tools/shop.py`, `INSTANCES` builds ports from `FULFILLMENT_PORT`; make the range 7 and call `shop.start_fulfillment(["f4"])`.</details>

<details class="sdc-hint"><summary>Hint 2</summary>With 6 partitions and 4 members, two members own 2 partitions and two own 1. Check `ownedPartitions` in each `/stats`.</details>

<details class="sdc-hint"><summary>Hint 3</summary>The seventh member owns nothing: it is a hot standby that takes over immediately when another member leaves.</details>

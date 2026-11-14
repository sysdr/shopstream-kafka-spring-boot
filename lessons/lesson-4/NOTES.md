A customer clicks "Place order", order-service says "done", and the order never reaches the warehouse. Nothing crashed in your code; a broker did, and the producer had been told the write was safe when it was not. Today you run three brokers, kill the leader in the middle of 20,000 orders, and count exactly how many acknowledged orders vanish or arrive twice under three producer settings.

## What you build

- **A 3-broker KRaft cluster** on ports 19092, 19192 and 19292. Every partition of `orders` has **3 replicas** and `min.insync.replicas=2`.
- **Three producer configurations** in `ProducerModes`, so the same burst can be replayed with each:

| Mode | `acks` | `enable.idempotence` |
|---|---|---|
| `acks1` | 1 (leader only) | false |
| `acksAll` | all (every in-sync replica) | false |
| `safe` | all | true |

- `POST /bursts` publishes a paced burst (here 20,000 orders at 5,000/s) and records **which order ids the brokers acknowledged**. Those ids are promises.
- fulfillment-service counts, per run, events received and distinct order ids. **Lost** = acknowledged but never delivered. **Duplicates** = delivered more than once.

## The drill

```python
victim = shop.cluster()["leaders"]["0"]      # the broker leading partition 0
shop.burst(run_id, mode, 20000, 5000)
time.sleep(1.5)
shop.kill_broker(victim)                       # kill -9: no clean shutdown
```

After the burst, the broker is restarted, the demo waits until every partition is fully replicated again, lets the consumer settle, and compares the acknowledged ids with the delivered ids.

## What we measured

In the browser sandbox, one kill per mode:

| Mode | Acknowledged | Failed | Lost | Duplicates |
|---|---|---|---|---|
| `acks1` | 20,000 | 0 | **65** | 0 |
| `acksAll` | 20,000 | 0 | 0 | **449** |
| `safe` | 20,000 | 0 | **0** | **0** |

The killed broker was back with all replicas in sync about 20 s after the kill each time (19.1–21.6 s).

Read the first row carefully: the producer got **no errors**. All 20,000 sends succeeded. Yet 65 orders that the brokers acknowledged never reached fulfillment.

## Why acks=1 loses acknowledged writes

With `acks=1` the leader replies as soon as the record is in its own log. Followers copy it a few milliseconds later. Kill the leader inside that window and a follower that has not yet copied the tail becomes the new leader. When the old leader returns, it truncates its log to match the new leader. The records only it had are gone — after it told the producer they were safe.

## Why acks=all without idempotence duplicates

With `acks=all` the leader waits until every in-sync replica has the record, so a surviving follower always has it. But the kill also cuts connections mid-request. The producer did not get the ack, so it retries — to the new leader, which already has the record. Without idempotence the broker cannot tell a retry from a new order: 449 orders arrived twice.

## Why the safe producer gets both right

With `enable.idempotence=true` the producer gets a producer id and numbers every batch per partition. The broker keeps the last sequence numbers it accepted and silently drops a retry it has already written. `acks=all` stops the loss; idempotence stops the duplicate. Kafka 3.x enables idempotence by default — unless you set `acks=1` or `retries=0`, which turns it off.

`min.insync.replicas=2` is the other half: with only one replica left, the broker refuses `acks=all` writes instead of pretending they are safe. You trade availability for not lying to the producer.

## Try it

```bash
curl -s localhost:8081/cluster
curl -s -X POST localhost:8081/bursts -H 'Content-Type: application/json' \
  -d '{"runId":"mine","mode":"acks1","count":5000,"ratePerSec":2000}'
kill -9 $(cat shopstream-events/.run/kafka-1.pid)
curl -s localhost:8081/bursts/mine; curl -s localhost:8082/runs/mine
bash shopstream-events/.run/kafka-1/start-broker.sh
```

## Say it in an interview

**Q: Can Kafka lose a message that the producer was told was written?**
A: Yes, with `acks=1`: the leader acknowledges before followers copy the record, and if it dies in that window the new leader never had it. Use `acks=all` with `min.insync.replicas=2` and a replication factor of 3.

**Q: Why does `acks=all` alone not give exactly-once delivery to the log?**
A: A timeout or lost connection makes the producer retry a write that may already be stored, creating a duplicate. The idempotent producer adds a producer id and sequence numbers so the broker drops such retries.

**Q: What does `min.insync.replicas` trade?**
A: Durability for availability. With 3 replicas and min ISR 2, one broker can be down and writes continue; with two down, `acks=all` writes fail rather than land on a single copy.

## Recap

- The producer's "success" is only as good as `acks`.
- Measured with a leader killed mid-burst: `acks=1` lost 65 acknowledged orders; `acks=all` without idempotence delivered 449 twice; `acks=all` + idempotence lost 0 and duplicated 0.
- Replication factor 3 + `min.insync.replicas=2` + `acks=all` + idempotence is the baseline for order data.
- A killed broker rejoined and was fully in sync about 20 s later.

## Challenge (10 minutes)

Kill **two** brokers during a `safe` burst. What happens to the producer, and what happens once you restart one of them?

<details class="sdc-hint"><summary>Hint 1</summary>With one replica left the ISR is smaller than `min.insync.replicas`, so the broker rejects `acks=all` writes with `NotEnoughReplicas`.</details>

<details class="sdc-hint"><summary>Hint 2</summary>Watch `failed` in `/bursts/RUN` grow once the producer's `delivery.timeout.ms` (30 s here) runs out.</details>

<details class="sdc-hint"><summary>Hint 3</summary>Restart one broker with its `start-broker.sh`; once the ISR is back to 2, new writes succeed again, and the orders that failed were never acknowledged — no lie, just an error the caller can handle.</details>

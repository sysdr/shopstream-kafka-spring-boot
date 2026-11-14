"""Day 6 demo: retention, a poison message, a lag alert and a broker failure drill."""
import time

import shop

print("1) Topic settings")
t = shop.call("GET", shop.ORDERS_URL + "/ops/topic")
print(f"   retention.ms={t['retention.ms']} ({int(t['retention.ms']) / 86400000:.0f} days)  "
      f"min.insync.replicas={t['min.insync.replicas']}  cleanup.policy={t['cleanup.policy']}")

print("\n2) Poison messages: 5 unreadable events mixed into 300 good orders")
d0 = shop.dlt()["deadLetters"]
run = f"good{int(time.time()) % 100000}"
shop.burst(run, "safe", 150, 1000)
shop.call("POST", shop.ORDERS_URL + "/ops/poison?count=5")
run2 = run + "b"
shop.burst(run2, "safe", 150, 1000)
shop.wait_for(lambda: shop.dlt()["deadLetters"] - d0 >= 5, timeout=30)
good = shop.settle(run, quiet=2) + shop.settle(run2, quiet=2)
d = shop.dlt()
print(f"   good orders processed {good}/300   dead letters {d['deadLetters'] - d0}   consumer lag {shop.lag_total()}")
if d["recent"]:
    r = d["recent"][0]
    print(f"   e.g. key={r['key']} from partition {r['originalPartition']} offset {r['originalOffset']}: {r['cause']}")

print("\n3) Lag alert: fulfillment stops while orders keep coming (threshold 500, for 5 s)")
shop.stop_service("f1")
t0 = time.time()
run = f"lag{int(time.time()) % 100000}"
shop.burst(run, "safe", 3000, 300)
fired = None
while time.time() - t0 < 25:
    a = shop.alert()
    if a["state"] == "FIRING" and fired is None:
        fired = time.time() - t0
    time.sleep(0.5)
print(f"   alert FIRING {fired:.1f} s after the consumer stopped (lag {shop.alert()['lag']})" if fired else
      "   alert did not fire")
shop.start_fulfillment(["f1"], ["--app.consumer.concurrency=3", "--app.process-delay-ms=0"])
t1 = time.time()
shop.wait_for(lambda: shop.alert()["state"] == "OK", timeout=60, every=0.5)
print(f"   consumer restarted; alert back to OK {time.time() - t1:.1f} s later")
for line in shop.alert()["transitions"][-3:]:
    print("     " + line)
shop.wait_for(lambda: shop.burst_status(run)["done"], timeout=30)

print("\n4) Broker failure drill: 200 orders/s for 20 s, kill -9 the leader of partition 0 after 5 s")
shop.wait_for(lambda: shop.cluster()["underReplicated"] == 0, timeout=90)
victim = shop.cluster()["leaders"]["0"]
run = f"drill{int(time.time()) % 100000}"
shop.burst(run, "safe", 4000, 200)
time.sleep(5)
shop.kill_broker(victim)
t_kill = time.time()
peak, back = 0, None
while time.time() - t_kill < 30:
    try:
        lag = shop.lag_total()
    except Exception:
        time.sleep(0.5)
        continue
    peak = max(peak, lag)
    if back is None and time.time() - t_kill > 1 and lag < 100:
        back = time.time() - t_kill
    time.sleep(0.5)
shop.wait_for(lambda: shop.burst_status(run)["done"], timeout=60)
b = shop.burst_status(run, ids=True)
shop.start_broker(victim)
shop.wait_for(lambda: shop.cluster()["underReplicated"] == 0, timeout=120, every=1)
isr_back = time.time() - t_kill
shop.settle(run, quiet=3)
got = shop.consumed(run, ids=True)
lost = len(set(b["ackedIds"]) - set(got["ids"]))
print(f"   producer: acknowledged {b['acked']}/4000  failed {b['failed']}  longest stall {b['maxAckGapMs']} ms")
print(f"   consumer: peak lag {peak}  back under 100 {back:.1f} s after the kill" if back is not None else
      f"   consumer: peak lag {peak}")
print(f"   broker {victim} restarted, all replicas in sync {isr_back:.1f} s after the kill")
print(f"   lost {lost}   duplicates {got['duplicates']}")

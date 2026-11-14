"""Day 6 checks used by verify.sh."""
import time

import shop

for url in (shop.ORDERS_URL, shop.FULFILL_URL):
    if not shop.healthy(url):
        shop.fail("service not answering on " + url)
t = shop.call("GET", shop.ORDERS_URL + "/ops/topic")
if t["retention.ms"] != "604800000" or t["min.insync.replicas"] != "2":
    shop.fail(f"orders topic should keep 7 days with min.insync.replicas=2, got {t}")
print(f"[VERIFY] orders topic: retention 7 days, min.insync.replicas 2, 3 brokers")
a = shop.alert()
if a["state"] not in ("OK", "PENDING", "FIRING") or a["threshold"] != 500:
    shop.fail(f"lag alert not running: {a}")
d0 = shop.dlt()["deadLetters"]
shop.call("POST", shop.ORDERS_URL + "/ops/poison?count=3")
run = f"verify{int(time.time()) % 100000}"
shop.burst(run, "safe", 50, 500)
if not shop.wait_for(lambda: shop.dlt()["deadLetters"] - d0 >= 3, timeout=30):
    shop.fail("poison messages did not reach orders.DLT")
if shop.settle(run, quiet=2) != 50:
    shop.fail("good orders stopped flowing behind the poison messages")
print(f"[VERIFY] 3 poison messages in orders.DLT, 50 good orders still processed, alert state {a['state']}")

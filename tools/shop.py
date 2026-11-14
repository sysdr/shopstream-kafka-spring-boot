"""Small helpers shared by the demo and the checks (Python standard library only)."""
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
RUN = PROJECT / ".run"
ORDERS_URL = f"http://127.0.0.1:{os.environ.get('ORDERS_PORT', '8081')}"
FULFILL_PORT = int(os.environ.get("FULFILLMENT_PORT", "8082"))
FULFILL_URL = f"http://127.0.0.1:{FULFILL_PORT}"
# Day 3: three fulfillment instances in one consumer group
INSTANCES = {f"f{i + 1}": FULFILL_PORT + i for i in range(3)}
KAFKA_HOME = Path(os.environ.get("KAFKA_HOME", Path.home() / ".cache/shopstream/kafka_2.13-3.9.1"))
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")


def call(method, url, body=None, timeout=10):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read() or b"{}")


def place_order(i):
    return call("POST", ORDERS_URL + "/orders",
                {"customerId": f"C-{i % 50}", "sku": f"SKU-{i % 20}", "quantity": 1 + i % 3, "price": 19.99})


def stats():
    return call("GET", FULFILL_URL + "/stats")


def wait_for(predicate, timeout=30, every=0.2):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(every)
    return False


def healthy(url):
    try:
        return call("GET", url + "/health", timeout=2).get("status") == "ok"
    except Exception:
        return False


def start_service(name, profile, port, extra=()):
    """Start one role of the jar in the background; pid in .run/NAME.pid, log in .run/NAME.log."""
    RUN.mkdir(exist_ok=True)
    log = open(RUN / f"{name}.log", "ab")
    cmd = ["java", "-Xmx256m", "-jar", str(PROJECT / "target/shopstream-events.jar"),
           f"--spring.profiles.active={profile}", f"--server.port={port}", *extra]
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, cwd=PROJECT)
    (RUN / f"{name}.pid").write_text(str(p.pid))
    return p.pid


def stop_service(name):
    pid_file = RUN / f"{name}.pid"
    if not pid_file.exists():
        return False
    pid = int(pid_file.read_text())
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(50):
            os.kill(pid, 0)
            time.sleep(0.1)
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    pid_file.unlink(missing_ok=True)
    return True


def consumer_lag(group):
    """Total lag of a consumer group, read with Kafka's own CLI."""
    out = subprocess.run([str(KAFKA_HOME / "bin/kafka-consumer-groups.sh"), "--bootstrap-server", BOOTSTRAP,
                          "--describe", "--group", group], capture_output=True, text=True, timeout=60).stdout
    lag = 0
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == group and parts[5].isdigit():
            lag += int(parts[5])
    return lag


def fail(msg):
    print(f"[FAIL] {msg}")
    sys.exit(1)


def reset():
    return call("POST", FULFILL_URL + "/stats/reset")


def simulate(orders, key_mode):
    return call("POST", ORDERS_URL + "/orders/simulate", {"orders": orders, "keyMode": key_mode}, timeout=120)


def instance_url(name):
    return f"http://127.0.0.1:{INSTANCES[name]}"


def start_fulfillment(names, extra=()):
    for n in names:
        start_service(n, "fulfillment", INSTANCES[n], [f"--app.instance={n}", *extra])
    for n in names:
        if not wait_for(lambda: healthy(instance_url(n)), timeout=90):
            fail(f"fulfillment instance {n} did not start; see .run/{n}.log")


def stop_fulfillment(names=None):
    for n in names or INSTANCES:
        stop_service(n)


def group_stats(names):
    """Sum the per-instance counters of the running fulfillment instances."""
    per = {n: call("GET", instance_url(n) + "/stats") for n in names}
    total = {k: sum(p[k] for p in per.values()) for k in ("events", "rebalances", "outOfOrder")}
    return total, per


def reset_group(names):
    for n in names:
        call("POST", instance_url(n) + "/stats/reset")


def lag(names):
    return call("GET", instance_url(names[0]) + "/lag")


def cluster():
    return call("GET", ORDERS_URL + "/cluster")


def kill_broker(node_id):
    """kill -9 one broker: no clean shutdown, no leadership hand-over."""
    pid = int((RUN / f"kafka-{node_id}.pid").read_text())
    os.kill(pid, signal.SIGKILL)
    return pid


def start_broker(node_id):
    subprocess.run(["bash", str(RUN / f"kafka-{node_id}" / "start-broker.sh")], check=True)


def burst(run_id, mode, count, rate):
    return call("POST", ORDERS_URL + "/bursts", {"runId": run_id, "mode": mode, "count": count, "ratePerSec": rate})


def burst_status(run_id, ids=False):
    return call("GET", f"{ORDERS_URL}/bursts/{run_id}?ids={'true' if ids else 'false'}", timeout=30)


def consumed(run_id, ids=False):
    return call("GET", f"{FULFILL_URL}/runs/{run_id}?ids={'true' if ids else 'false'}", timeout=30)


def settle(run_id, quiet=3.0, timeout=90):
    """Wait until the consumer has seen no new events for this run for `quiet` seconds."""
    last, since, end = -1, time.time(), time.time() + timeout
    while time.time() < end:
        n = consumed(run_id)["received"]
        if n != last:
            last, since = n, time.time()
        elif time.time() - since >= quiet:
            return n
        time.sleep(0.5)
    return last


def orders_alive():
    return healthy(ORDERS_URL)


def restart_orders(extra=()):
    stop_service("orders")
    start_service("orders", "orders", ORDERS_URL.rsplit(":", 1)[1], list(extra))
    if not wait_for(orders_alive, timeout=90):
        fail("order-service did not restart; see .run/orders.log")


def db_ids(prefix):
    return call("GET", f"{ORDERS_URL}/orders/ids?prefix={prefix}", timeout=30)


def outbox():
    return call("GET", ORDERS_URL + "/outbox")


def alert():
    return call("GET", ORDERS_URL + "/ops/alert")


def dlt():
    return call("GET", FULFILL_URL + "/ops/dlt")


def lag_total():
    return call("GET", FULFILL_URL + "/lag")["total"]


def wait_stable(names, timeout=60):
    """Wait until the group has settled: every instance owns partitions, together exactly 0-5, no rebalance for 2 s."""
    end, last, since = time.time() + timeout, None, time.time()
    while time.time() < end:
        try:
            per = {n: call("GET", instance_url(n) + "/stats") for n in names}
        except Exception:
            time.sleep(0.5)
            continue
        owned = sorted(p for n in names for p in per[n]["ownedPartitions"])
        sig = (tuple(owned), sum(per[n]["rebalances"] for n in names))
        if sig != last:
            last, since = sig, time.time()
        elif owned == list(range(6)) and all(per[n]["ownedPartitions"] for n in names) and time.time() - since >= 2:
            return True
        time.sleep(0.5)
    return False


def skip_backlog(group="fulfillment"):
    """Move a stopped group's offsets to the end of the topic (discard what it has not read)."""
    subprocess.run([str(KAFKA_HOME / "bin/kafka-consumer-groups.sh"), "--bootstrap-server", BOOTSTRAP, "--group", group,
                    "--reset-offsets", "--to-latest", "--all-topics", "--execute"], capture_output=True, timeout=60)

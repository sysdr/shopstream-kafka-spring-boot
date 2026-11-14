#!/usr/bin/env bash
# ShopStream events — Day 1: Publish your first OrderPlaced event
# Downloads Kafka (once, cached in ~/.cache/shopstream), starts a 1-broker KRaft cluster,
# writes the Spring Boot project into ./shopstream-events, builds and tests it with Maven,
# starts order-service and fulfillment-service, writes verify.sh, verifies, then runs the demo.
# Stop everything with: bash stop.sh
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$ROOT/shopstream-events"
RUN="$PROJECT_DIR/.run"
cd "$ROOT"
KAFKA_VERSION="3.9.1"
KAFKA_HOME="${KAFKA_HOME:-$HOME/.cache/shopstream/kafka_2.13-$KAFKA_VERSION}"
BROKERS=1
export KAFKA_HOME ORDERS_PORT="${ORDERS_PORT:-8081}" FULFILLMENT_PORT="${FULFILLMENT_PORT:-8082}"
export KAFKA_BOOTSTRAP="localhost:19092$( [ "$BROKERS" -gt 1 ] && echo ',localhost:29092,localhost:39092' || true )"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
need() { command -v "$1" >/dev/null || { echo "[ERROR] $1 not found. $2"; exit 1; }; }
need java "Install a JDK 17 or newer (e.g. sudo apt install openjdk-21-jdk)."
need mvn "Install Maven 3.8+ (e.g. sudo apt install maven)."
need curl "Install curl."
need python3 "Install Python 3.10+."
JAVA_MAJOR="$(java -XshowSettings:properties -version 2>&1 | awk -F'= ' '/java.specification.version/ {print $2}' | cut -d. -f1)"
[ "${JAVA_MAJOR:-0}" -ge 17 ] || { echo "[ERROR] Java 17+ is required (found ${JAVA_MAJOR:-unknown})."; exit 1; }

# Stop a previous run (idempotent): services first, then brokers.
if [ -f "$ROOT/stop.sh" ]; then bash "$ROOT/stop.sh" >/dev/null 2>&1 || true; fi

say "Kafka $KAFKA_VERSION"
if [ ! -x "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
  mkdir -p "$(dirname "$KAFKA_HOME")"
  TGZ="$(dirname "$KAFKA_HOME")/kafka_2.13-$KAFKA_VERSION.tgz"
  for URL in "https://archive.apache.org/dist/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz" \
             "https://downloads.apache.org/kafka/$KAFKA_VERSION/kafka_2.13-$KAFKA_VERSION.tgz"; do
    echo "[INFO] downloading $URL"
    if curl -fL --retry 2 -o "$TGZ.part" "$URL"; then mv "$TGZ.part" "$TGZ"; break; fi
  done
  [ -f "$TGZ" ] || { echo "[ERROR] could not download Kafka"; exit 1; }
  tar -xzf "$TGZ" -C "$(dirname "$KAFKA_HOME")" && rm -f "$TGZ"
fi
echo "[INFO] Kafka at $KAFKA_HOME"

mkdir -p "$RUN"
rm -rf "$RUN"/kafka-*   # fresh cluster every run: the demo numbers start from zero
VOTERS=""
for i in $(seq 1 "$BROKERS"); do VOTERS="$VOTERS${VOTERS:+,}$i@localhost:${i}9093"; done
CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
RF=$(( BROKERS >= 3 ? 3 : 1 )); MINISR=$(( BROKERS >= 3 ? 2 : 1 ))
for i in $(seq 1 "$BROKERS"); do
  D="$RUN/kafka-$i"; mkdir -p "$D/data" "$D/logs"
  cat > "$D/server.properties" <<PROPS
process.roles=broker,controller
node.id=$i
controller.quorum.voters=$VOTERS
listeners=PLAINTEXT://localhost:${i}9092,CONTROLLER://localhost:${i}9093
advertised.listeners=PLAINTEXT://localhost:${i}9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$D/data
num.partitions=3
default.replication.factor=$RF
min.insync.replicas=$MINISR
offsets.topic.replication.factor=$RF
transaction.state.log.replication.factor=$RF
transaction.state.log.min.isr=$MINISR
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
PROPS
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$D/server.properties" >/dev/null
  LOG_DIR="$D/logs" KAFKA_HEAP_OPTS="-Xmx512m -Xms256m" nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$D/server.properties" > "$D/logs/console.log" 2>&1 &
  echo $! > "$RUN/kafka-$i.pid"
done
for _ in $(seq 1 60); do
  if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" >/dev/null 2>&1; then break; fi
  sleep 1
done
"$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server "$KAFKA_BOOTSTRAP" >/dev/null 2>&1 || { echo "[ERROR] Kafka did not start:"; tail -20 "$RUN/kafka-1/logs/console.log"; exit 1; }
echo "[INFO] $BROKERS broker(s) up: $KAFKA_BOOTSTRAP"

say "Writing the project into ./shopstream-events"
rm -rf "$PROJECT_DIR/src" "$PROJECT_DIR/tools"
mkdir -p "$PROJECT_DIR/."
cat > "$PROJECT_DIR/pom.xml" <<'SHOPSTREAM_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.0</version>
    <relativePath/>
  </parent>
  <groupId>com.shopstream</groupId>
  <artifactId>shopstream-events</artifactId>
  <version>1.0.0</version>
  <name>shopstream-events</name>
  <properties>
    <java.version>17</java.version>
  </properties>
  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.kafka</groupId>
      <artifactId>spring-kafka</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
  <build>
    <finalName>shopstream-events</finalName>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/FulfillmentService.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** fulfillment-service: consumes OrderPlaced and measures how long each event took to arrive. */
@RestController
@Profile("fulfillment")
public class FulfillmentService {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);
    private final ObjectMapper json;
    private final LatencyStats stats = new LatencyStats();

    public FulfillmentService(ObjectMapper json) {
        this.json = json;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "fulfillment")
    public void onOrderPlaced(String value) throws Exception {
        OrderPlaced event = json.readValue(value, OrderPlaced.class);
        stats.record(event.orderId(), System.currentTimeMillis() - event.createdAtMs());
        log.info("fulfilling {} ({} x {})", event.orderId(), event.quantity(), event.sku());
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return stats.snapshot();
    }

    @PostMapping("/stats/reset")
    public Map<String, Object> reset() {
        stats.reset();
        return stats.snapshot();
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/HealthController.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.Map;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {
    private final Environment env;

    public HealthController(Environment env) {
        this.env = env;
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        return Map.of("status", "ok", "role", String.join(",", env.getActiveProfiles()));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/LatencyStats.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Thread-safe store of end-to-end latencies (producer timestamp to consumer receive). */
public class LatencyStats {
    private final List<Long> samples = new ArrayList<>();
    private String lastOrderId = "";

    public synchronized void record(String orderId, long latencyMs) {
        samples.add(Math.max(0, latencyMs));
        lastOrderId = orderId;
    }

    public synchronized void reset() {
        samples.clear();
        lastOrderId = "";
    }

    public synchronized int count() {
        return samples.size();
    }

    /** Nearest-rank percentile; 0 when there are no samples. */
    public synchronized long percentile(double p) {
        if (samples.isEmpty()) {
            return 0;
        }
        List<Long> sorted = new ArrayList<>(samples);
        Collections.sort(sorted);
        int rank = (int) Math.ceil(p / 100.0 * sorted.size());
        return sorted.get(Math.min(sorted.size() - 1, Math.max(0, rank - 1)));
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("received", samples.size());
        out.put("p50Ms", percentile(50));
        out.put("p99Ms", percentile(99));
        out.put("maxMs", samples.isEmpty() ? 0 : Collections.max(samples));
        out.put("lastOrderId", lastOrderId);
        return out;
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/OrderController.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** order-service: accepts an order over HTTP and publishes OrderPlaced to the "orders" topic. */
@RestController
@Profile("orders")
public class OrderController {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final AtomicLong nextId = new AtomicLong(1001);

    public OrderController(KafkaTemplate<String, String> kafka, ObjectMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    public record NewOrder(String customerId, String sku, int quantity, double price) {
    }

    @PostMapping("/orders")
    public Map<String, Object> place(@RequestBody NewOrder req) throws Exception {
        String orderId = "ORD-" + nextId.getAndIncrement();
        OrderPlaced event = new OrderPlaced(orderId, req.customerId(), req.sku(), req.quantity(),
                Math.round(req.quantity() * req.price() * 100) / 100.0, System.currentTimeMillis());
        // Wait for the broker's acknowledgement so the caller knows the event is stored.
        SendResult<String, String> sent = kafka.send(Topics.ORDERS, orderId, json.writeValueAsString(event))
                .get(10, TimeUnit.SECONDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("orderId", orderId);
        out.put("partition", sent.getRecordMetadata().partition());
        out.put("offset", sent.getRecordMetadata().offset());
        return out;
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/OrderPlaced.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

/** The event published when a customer places an order. createdAtMs is the producer's clock. */
public record OrderPlaced(String orderId, String customerId, String sku, int quantity, double total, long createdAtMs) {
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/ShopstreamEventsApplication.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * One jar, two services. Pick the role with a Spring profile:
 *   --spring.profiles.active=orders       order-service: POST /orders publishes OrderPlaced
 *   --spring.profiles.active=fulfillment  fulfillment-service: consumes OrderPlaced
 */
@SpringBootApplication
public class ShopstreamEventsApplication {
    public static void main(String[] args) {
        SpringApplication.run(ShopstreamEventsApplication.class, args);
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/Topics.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Spring's KafkaAdmin creates these topics at start-up if they do not exist. */
@Configuration
public class Topics {
    public static final String ORDERS = "orders";

    @Bean
    public NewTopic ordersTopic(@Value("${app.orders.partitions:3}") int partitions) {
        return TopicBuilder.name(ORDERS).partitions(partitions).replicas(1).build();
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/resources"
cat > "$PROJECT_DIR/src/main/resources/application.properties" <<'SHOPSTREAM_EOF'
spring.application.name=shopstream-events
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP:localhost:19092}
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.listener.missing-topics-fatal=false
spring.main.banner-mode=off
logging.level.root=WARN
logging.level.com.shopstream=INFO
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/test/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/LatencyStatsTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class LatencyStatsTest {
    @Test
    void emptyStatsAreZero() {
        LatencyStats s = new LatencyStats();
        assertEquals(0, s.count());
        assertEquals(0, s.percentile(99));
    }

    @Test
    void nearestRankPercentiles() {
        LatencyStats s = new LatencyStats();
        for (int i = 1; i <= 100; i++) {
            s.record("ORD-" + i, i);
        }
        assertEquals(50, s.percentile(50));
        assertEquals(99, s.percentile(99));
        assertEquals(100, s.percentile(100));
        assertEquals("ORD-100", s.snapshot().get("lastOrderId"));
    }

    @Test
    void negativeClockSkewCountsAsZero() {
        LatencyStats s = new LatencyStats();
        s.record("ORD-1", -5);
        assertEquals(0, s.percentile(50));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/test/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/OrderPlacedJsonTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class OrderPlacedJsonTest {
    @Test
    void roundTripsThroughJson() throws Exception {
        ObjectMapper json = new ObjectMapper();
        OrderPlaced e = new OrderPlaced("ORD-1001", "C-7", "SKU-42", 2, 39.98, 1700000000000L);
        String text = json.writeValueAsString(e);
        assertEquals(e, json.readValue(text, OrderPlaced.class));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/checks.py" <<'SHOPSTREAM_EOF'
"""Day 1 checks used by verify.sh."""
import shop

if not shop.healthy(shop.ORDERS_URL):
    shop.fail("order-service is not answering on " + shop.ORDERS_URL)
if not shop.healthy(shop.FULFILL_URL):
    shop.fail("fulfillment-service is not answering on " + shop.FULFILL_URL)
print("[VERIFY] both services healthy")

before = shop.stats()["received"]
first = shop.place_order(1)
if not first.get("orderId", "").startswith("ORD-") or "offset" not in first:
    shop.fail(f"POST /orders did not return an orderId and offset: {first}")
for i in range(19):
    shop.place_order(2 + i)
if not shop.wait_for(lambda: shop.stats()["received"] >= before + 20, timeout=20):
    shop.fail(f"fulfillment received {shop.stats()['received'] - before} of 20 events")
s = shop.stats()
print(f"[VERIFY] 20 events published and consumed; p99 end-to-end {s['p99Ms']} ms")
if s["p99Ms"] > 5000:
    shop.fail("p99 end-to-end latency is over 5 s (first events include JVM warm-up)")
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/demo.py" <<'SHOPSTREAM_EOF'
"""Day 1 demo: measure end-to-end latency, then stop the consumer and watch events wait."""
import time

import shop

N = 200
print(f"Publishing {N} OrderPlaced events through order-service ...")
shop.call("POST", shop.FULFILL_URL + "/stats/reset")
t0 = time.time()
for i in range(N):
    shop.place_order(i)
publish_s = time.time() - t0
shop.wait_for(lambda: shop.stats()["received"] >= N, timeout=30)
s = shop.stats()
print(f"  published {N} in {publish_s:.2f} s ({N / publish_s:.0f} orders/s, each waits for the broker ack)")
print(f"  fulfillment received {s['received']}  end-to-end p50 {s['p50Ms']} ms  p99 {s['p99Ms']} ms  max {s['maxMs']} ms")

print("\nBreak it: stop fulfillment-service, keep taking orders ...")
shop.stop_service("fulfillment")
shop.call("POST", shop.ORDERS_URL + "/orders", {"customerId": "C-1", "sku": "SKU-1", "quantity": 1, "price": 1.0})
for i in range(49):
    shop.place_order(1000 + i)
time.sleep(1)
lag = shop.consumer_lag("fulfillment")
print(f"  50 orders accepted while the consumer was down; consumer-group lag = {lag} (events waiting in Kafka)")

print("Restart fulfillment-service ...")
t1 = time.time()
shop.start_service("fulfillment", "fulfillment", shop.FULFILL_URL.rsplit(":", 1)[1])
shop.wait_for(lambda: shop.healthy(shop.FULFILL_URL), timeout=60)
shop.wait_for(lambda: shop.stats()["received"] >= 50, timeout=60)
caught_up = time.time() - t1
s = shop.stats()
print(f"  caught up: received {s['received']} waiting events {caught_up:.1f} s after restart (includes JVM start-up)")
print(f"  lag now = {shop.consumer_lag('fulfillment')}; nothing was lost, the oldest event waited {s['maxMs'] / 1000:.1f} s")
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/shop.py" <<'SHOPSTREAM_EOF'
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
FULFILL_URL = f"http://127.0.0.1:{os.environ.get('FULFILLMENT_PORT', '8082')}"
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
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
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
SHOPSTREAM_EOF

say "Build and unit tests (Maven)"
(cd "$PROJECT_DIR" && mvn -q -B package 2>&1 | tail -30) 
[ -f "$PROJECT_DIR/target/shopstream-events.jar" ] || { echo "[ERROR] build failed: cd shopstream-events && mvn package"; exit 1; }
echo "[INFO] tests passed, built target/shopstream-events.jar"

say "Starting services"
python3 - <<'START_PY'
import sys
sys.path.insert(0, "shopstream-events/tools")
import shop
shop.start_service("orders", "orders", shop.ORDERS_URL.rsplit(":", 1)[1])
shop.start_service("fulfillment", "fulfillment", shop.FULFILL_URL.rsplit(":", 1)[1])
for url in [shop.ORDERS_URL, shop.FULFILL_URL]:
    if not shop.wait_for(lambda: shop.healthy(url), timeout=90):
        print("[ERROR] service did not start:", url); print(open(shop.RUN / "fulfillment.log").read()[-2000:]); sys.exit(1)
print("[INFO] services healthy")
START_PY

cat > "$ROOT/verify.sh" <<'VERIFY_EOF'
#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
export KAFKA_HOME="${KAFKA_HOME:-$HOME/.cache/shopstream/kafka_2.13-3.9.1}"
export KAFKA_BOOTSTRAP="localhost:19092"
python3 "$ROOT/shopstream-events/tools/checks.py"
echo "PASS  Lesson 1 complete. Your completion code: SHOP2-PUBLISH-5R8K"
VERIFY_EOF
chmod +x "$ROOT/verify.sh"

say "Verify"
bash "$ROOT/verify.sh"

say "Demo"
python3 "$PROJECT_DIR/tools/demo.py"

say "Done"
echo "order-service:       http://localhost:$ORDERS_PORT   (POST /orders)"
echo "fulfillment-service: http://localhost:$FULFILLMENT_PORT   (GET /stats)"
echo "Run bash verify.sh for your completion code, bash stop.sh when you are finished."

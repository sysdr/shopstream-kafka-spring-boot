#!/usr/bin/env bash
# ShopStream events — Day 2: Keys, partitions and order
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
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/** fulfillment-service: consumes order events with several threads and checks their order. */
@RestController
@Profile("fulfillment")
public class FulfillmentService {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);
    private final ObjectMapper json;
    private final LatencyStats latency = new LatencyStats();
    private final OrderTracker tracker = new OrderTracker();
    private final long processDelayMs;

    public FulfillmentService(ObjectMapper json, @Value("${app.process-delay-ms:1}") long processDelayMs) {
        this.json = json;
        this.processDelayMs = processDelayMs;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "fulfillment", concurrency = "${app.consumer.concurrency:3}")
    public void onEvent(ConsumerRecord<String, String> rec) throws Exception {
        OrderEvent e = json.readValue(rec.value(), OrderEvent.class);
        latency.record(e.orderId(), System.currentTimeMillis() - e.createdAtMs());
        if (!tracker.record(e, rec.partition())) {
            log.warn("{} {} (seq {}) arrived out of order on partition {}", e.orderId(), e.type(), e.seq(), rec.partition());
        }
        if (processDelayMs > 0) {
            Thread.sleep(processDelayMs); // stands in for real work: reserve stock, print a label
        }
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> out = new LinkedHashMap<>(latency.snapshot());
        out.putAll(tracker.snapshot());
        return out;
    }

    @PostMapping("/stats/reset")
    public Map<String, Object> reset() {
        latency.reset();
        tracker.reset();
        return stats();
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
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/Keys.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.utils.Utils;

/** Which key an event is published with decides its partition, and so its order. */
public final class Keys {
    private Keys() {
    }

    /** "orderId" (correct: one order, one partition) or "eventId" (the bug: every event lands anywhere). */
    public static String keyFor(OrderEvent e, String mode) {
        return "eventId".equals(mode) ? e.eventId() : e.orderId();
    }

    /** The same formula Kafka's default partitioner uses for a non-null key. */
    public static int partitionFor(String key, int partitions) {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % partitions;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** order-service: publishes order events to the "orders" topic. */
@RestController
@Profile("orders")
public class OrderController {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final int partitions;
    private final AtomicLong nextId = new AtomicLong(1001);

    public OrderController(KafkaTemplate<String, String> kafka, ObjectMapper json,
                           @Value("${app.orders.partitions:6}") int partitions) {
        this.kafka = kafka;
        this.json = json;
        this.partitions = partitions;
    }

    public record NewOrder(String customerId, String sku, int quantity, double price) {
    }

    public record Simulation(int orders, String keyMode) {
    }

    private OrderEvent event(String orderId, int seq, NewOrder o) {
        return new OrderEvent(UUID.randomUUID().toString(), orderId, OrderEvent.LIFECYCLE[seq - 1], seq,
                o.customerId(), o.sku(), o.quantity(), Math.round(o.quantity() * o.price() * 100) / 100.0,
                System.currentTimeMillis());
    }

    private CompletableFuture<SendResult<String, String>> publish(OrderEvent e, String keyMode) throws Exception {
        return kafka.send(Topics.ORDERS, Keys.keyFor(e, keyMode), json.writeValueAsString(e));
    }

    /** Day 1: one OrderPlaced, keyed by order id; waits for the broker's acknowledgement. */
    @PostMapping("/orders")
    public Map<String, Object> place(@RequestBody NewOrder req) throws Exception {
        String orderId = "ORD-" + nextId.getAndIncrement();
        SendResult<String, String> sent = publish(event(orderId, 1, req), "orderId").get(10, TimeUnit.SECONDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("orderId", orderId);
        out.put("partition", sent.getRecordMetadata().partition());
        out.put("offset", sent.getRecordMetadata().offset());
        return out;
    }

    /** Day 2: N orders, each Placed then Paid then Shipped, published back to back with the chosen key. */
    @PostMapping("/orders/simulate")
    public Map<String, Object> simulate(@RequestBody Simulation sim) throws Exception {
        String mode = sim.keyMode() == null ? "orderId" : sim.keyMode();
        long t0 = System.nanoTime();
        List<CompletableFuture<SendResult<String, String>>> sends = new ArrayList<>();
        for (int i = 0; i < sim.orders(); i++) {
            String orderId = "ORD-" + nextId.getAndIncrement();
            NewOrder o = new NewOrder("C-" + (i % 50), "SKU-" + (i % 20), 1 + i % 3, 19.99);
            for (int seq = 1; seq <= 3; seq++) {
                sends.add(publish(event(orderId, seq, o), mode));
            }
        }
        CompletableFuture.allOf(sends.toArray(new CompletableFuture[0])).get(60, TimeUnit.SECONDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("orders", sim.orders());
        out.put("events", sends.size());
        out.put("keyMode", mode);
        out.put("ms", (System.nanoTime() - t0) / 1_000_000);
        return out;
    }

    /** Which partition an order id maps to (same murmur2 formula as the producer). */
    @GetMapping("/orders/{orderId}/partition")
    public Map<String, Object> partition(@PathVariable String orderId) {
        return Map.of("orderId", orderId, "partition", Keys.partitionFor(orderId, partitions), "partitions", partitions);
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/OrderEvent.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

/**
 * One step in an order's life: OrderPlaced (seq 1), OrderPaid (seq 2), OrderShipped (seq 3).
 * createdAtMs is the producer's clock, used to measure end-to-end latency.
 */
public record OrderEvent(String eventId, String orderId, String type, int seq, String customerId, String sku,
                         int quantity, double total, long createdAtMs) {
    public static final String[] LIFECYCLE = {"OrderPlaced", "OrderPaid", "OrderShipped"};
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/OrderTracker.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Checks each order's events arrive in lifecycle order (seq 1, 2, 3) and records which
 * partitions carried them. Thread-safe: several listener threads call record().
 */
public class OrderTracker {
    private final Map<String, Integer> lastSeq = new HashMap<>();
    private final Map<String, Set<Integer>> partitionsByOrder = new HashMap<>();
    private final Map<Integer, Integer> eventsByPartition = new TreeMap<>();
    private int received;
    private int outOfOrder;
    private int completed;

    /** Returns true when the event arrived in order. */
    public synchronized boolean record(OrderEvent e, int partition) {
        received++;
        eventsByPartition.merge(partition, 1, Integer::sum);
        partitionsByOrder.computeIfAbsent(e.orderId(), k -> new HashSet<>()).add(partition);
        int last = lastSeq.getOrDefault(e.orderId(), 0);
        boolean inOrder = e.seq() == last + 1;
        if (!inOrder) {
            outOfOrder++;
        }
        lastSeq.put(e.orderId(), Math.max(last, e.seq()));
        if (e.seq() == 3 && inOrder) {
            completed++;
        }
        return inOrder;
    }

    public synchronized void reset() {
        lastSeq.clear();
        partitionsByOrder.clear();
        eventsByPartition.clear();
        received = 0;
        outOfOrder = 0;
        completed = 0;
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("events", received);
        out.put("orders", lastSeq.size());
        out.put("outOfOrder", outOfOrder);
        out.put("ordersShippedInOrder", completed);
        out.put("ordersSplitAcrossPartitions", partitionsByOrder.values().stream().filter(s -> s.size() > 1).count());
        out.put("eventsByPartition", new TreeMap<>(eventsByPartition));
        return out;
    }
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
    public NewTopic ordersTopic(@Value("${app.orders.partitions:6}") int partitions) {
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
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/KeysTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class KeysTest {
    private OrderEvent ev(String eventId, int seq) {
        return new OrderEvent(eventId, "ORD-1001", OrderEvent.LIFECYCLE[seq - 1], seq, "C-1", "SKU-1", 1, 9.99, 0);
    }

    @Test
    void orderIdKeySendsEveryEventOfAnOrderToOnePartition() {
        assertEquals(Keys.keyFor(ev("a", 1), "orderId"), Keys.keyFor(ev("b", 2), "orderId"));
        assertEquals(Keys.partitionFor("ORD-1001", 6), Keys.partitionFor(Keys.keyFor(ev("c", 3), "orderId"), 6));
    }

    @Test
    void eventIdKeyChangesWithEveryEvent() {
        assertNotEquals(Keys.keyFor(ev("a", 1), "eventId"), Keys.keyFor(ev("b", 2), "eventId"));
    }

    @Test
    void partitionIsStableAndInRange() {
        int p = Keys.partitionFor("ORD-1001", 6);
        assertEquals(p, Keys.partitionFor("ORD-1001", 6));
        for (int i = 0; i < 1000; i++) {
            int q = Keys.partitionFor("ORD-" + i, 6);
            assertEquals(true, q >= 0 && q < 6);
        }
    }
}
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
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/OrderTrackerTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OrderTrackerTest {
    private OrderEvent ev(String order, int seq) {
        return new OrderEvent(order + "-" + seq, order, OrderEvent.LIFECYCLE[seq - 1], seq, "C-1", "SKU-1", 1, 9.99, 0);
    }

    @Test
    void inOrderEventsCompleteTheOrder() {
        OrderTracker t = new OrderTracker();
        t.record(ev("ORD-1", 1), 2);
        t.record(ev("ORD-1", 2), 2);
        t.record(ev("ORD-1", 3), 2);
        assertEquals(0, t.snapshot().get("outOfOrder"));
        assertEquals(1, t.snapshot().get("ordersShippedInOrder"));
        assertEquals(0L, t.snapshot().get("ordersSplitAcrossPartitions"));
    }

    @Test
    void paidBeforePlacedIsCountedOutOfOrder() {
        OrderTracker t = new OrderTracker();
        t.record(ev("ORD-2", 2), 0);
        t.record(ev("ORD-2", 1), 4);
        t.record(ev("ORD-2", 3), 1);
        assertEquals(2, t.snapshot().get("outOfOrder"));
        assertEquals(0, t.snapshot().get("ordersShippedInOrder"));
        assertEquals(1L, t.snapshot().get("ordersSplitAcrossPartitions"));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/checks.py" <<'SHOPSTREAM_EOF'
"""Day 2 checks used by verify.sh."""
import shop

for url in (shop.ORDERS_URL, shop.FULFILL_URL):
    if not shop.healthy(url):
        shop.fail("service not answering on " + url)
p1 = shop.call("GET", f"{shop.ORDERS_URL}/orders/ORD-1001/partition")["partition"]
p2 = shop.call("GET", f"{shop.ORDERS_URL}/orders/ORD-1001/partition")["partition"]
if p1 != p2:
    shop.fail("the same key mapped to two partitions")
shop.reset()
r = shop.simulate(100, "orderId")
if not shop.wait_for(lambda: shop.stats()["events"] >= r["events"], timeout=60):
    shop.fail(f"consumed {shop.stats()['events']} of {r['events']} events")
s = shop.stats()
print(f"[VERIFY] 100 orders, 300 events keyed by order id: out-of-order {s['outOfOrder']}, split orders {s['ordersSplitAcrossPartitions']}")
if s["outOfOrder"] != 0 or s["ordersSplitAcrossPartitions"] != 0 or s["ordersShippedInOrder"] != 100:
    shop.fail("events of one order must stay on one partition and arrive in order")
if len(s["eventsByPartition"]) < 3:
    shop.fail("orders should spread over several partitions")
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/demo.py" <<'SHOPSTREAM_EOF'
"""Day 2 demo: the same 300 orders, keyed by order id and then by event id."""
import shop

N = 300
for i in ("ORD-1001", "ORD-1002", "ORD-1003"):
    p = shop.call("GET", f"{shop.ORDERS_URL}/orders/{i}/partition")
    print(f"  key {i} -> partition {p['partition']} of {p['partitions']}")

results = {}
for mode in ("orderId", "eventId"):
    shop.reset()
    r = shop.simulate(N, mode)
    shop.wait_for(lambda: shop.stats()["events"] >= r["events"], timeout=60)
    s = shop.stats()
    results[mode] = s
    print(f"\nkey = {mode}: published {r['events']} events for {N} orders in {r['ms']} ms")
    print(f"  consumed {s['events']}  out-of-order {s['outOfOrder']}  orders shipped in order {s['ordersShippedInOrder']}/{N}"
          f"  orders split across partitions {s['ordersSplitAcrossPartitions']}")
    print(f"  events per partition {s['eventsByPartition']}  end-to-end p99 {s['p99Ms']} ms")

bad = results["eventId"]
print(f"\nWrong key: {bad['outOfOrder']} of {bad['events']} events ({100 * bad['outOfOrder'] / max(1, bad['events']):.1f}%) "
      f"arrived before the event they depend on; right key: {results['orderId']['outOfOrder']}.")
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


def reset():
    return call("POST", FULFILL_URL + "/stats/reset")


def simulate(orders, key_mode):
    return call("POST", ORDERS_URL + "/orders/simulate", {"orders": orders, "keyMode": key_mode}, timeout=120)
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
echo "PASS  Lesson 2 complete. Your completion code: SHOP2-KEYS-3M7W"
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

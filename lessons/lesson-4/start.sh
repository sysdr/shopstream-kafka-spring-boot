#!/usr/bin/env bash
# ShopStream events — Day 4: Never lose an order
# Downloads Kafka (once, cached in ~/.cache/shopstream), starts a 3-broker KRaft cluster,
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
BROKERS=3
export ORDERS_REPLICAS=$(( BROKERS >= 3 ? 3 : 1 ))
export KAFKA_HOME ORDERS_PORT="${ORDERS_PORT:-8081}" FULFILLMENT_PORT="${FULFILLMENT_PORT:-8082}"
export KAFKA_BOOTSTRAP="localhost:19092$( [ "$BROKERS" -gt 1 ] && echo ',localhost:19192,localhost:19292' || true )"

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
rm -rf "$RUN"/kafka-* "$RUN"/orders-db*   # fresh cluster and database every run: the demo numbers start from zero
VOTERS=""
# ports stay below 32768 (the ephemeral range), or an outgoing connection can steal one
port() { echo $(( 19092 + ($1 - 1) * 100 + $2 )); }
for i in $(seq 1 "$BROKERS"); do VOTERS="$VOTERS${VOTERS:+,}$i@localhost:$(port $i 1)"; done
CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"
RF=$(( BROKERS >= 3 ? 3 : 1 )); MINISR=$(( BROKERS >= 3 ? 2 : 1 ))
for i in $(seq 1 "$BROKERS"); do
  D="$RUN/kafka-$i"; mkdir -p "$D/data" "$D/logs"
  cat > "$D/server.properties" <<PROPS
process.roles=broker,controller
node.id=$i
controller.quorum.voters=$VOTERS
listeners=PLAINTEXT://localhost:$(port $i 0),CONTROLLER://localhost:$(port $i 1)
advertised.listeners=PLAINTEXT://localhost:$(port $i 0)
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
  # start-broker.sh restarts this broker after a failure drill (the demo uses it).
  cat > "$D/start-broker.sh" <<BROKER
#!/usr/bin/env bash
LOG_DIR="$D/logs" KAFKA_HEAP_OPTS="-Xmx512m -Xms256m" nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$D/server.properties" >> "$D/logs/console.log" 2>&1 &
echo \$! > "$RUN/kafka-$i.pid"
BROKER
  bash "$D/start-broker.sh"
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
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/BurstService.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Day 4: publish a paced burst of orders with one producer mode and remember exactly which
 * order ids the brokers acknowledged. Those acknowledged ids are promises: every one must reach
 * the consumer.
 */
@RestController
@Profile("orders")
public class BurstService {
    private final ProducerModes modes;
    private final ObjectMapper json;
    private final ExecutorService runner = Executors.newSingleThreadExecutor();
    private final Map<String, Run> runs = new ConcurrentHashMap<>();

    public BurstService(ProducerModes modes, ObjectMapper json) {
        this.modes = modes;
        this.json = json;
    }

    public record Burst(String runId, String mode, int count, int ratePerSec) {
    }

    static final class Run {
        final Set<String> acked = ConcurrentHashMap.newKeySet();
        final AtomicInteger failed = new AtomicInteger();
        final AtomicInteger sent = new AtomicInteger();
        volatile boolean done;
        volatile long ms;
        volatile String error = "";
    }

    @PostMapping("/bursts")
    public Map<String, Object> start(@RequestBody Burst b) {
        modes.template(b.mode()); // fail fast on a bad mode
        Run run = new Run();
        runs.put(b.runId(), run);
        runner.submit(() -> publish(b, run));
        return Map.of("runId", b.runId(), "started", true);
    }

    private void publish(Burst b, Run run) {
        long t0 = System.nanoTime();
        long perEventNanos = 1_000_000_000L / Math.max(1, b.ratePerSec());
        try {
            for (int i = 1; i <= b.count(); i++) {
                String orderId = String.format("%s-%06d", b.runId(), i);
                OrderEvent e = new OrderEvent(UUID.randomUUID().toString(), orderId, "OrderPlaced", 1,
                        "C-" + (i % 50), "SKU-" + (i % 20), 1, 19.99, System.currentTimeMillis());
                modes.template(b.mode()).send(Topics.ORDERS, orderId, json.writeValueAsString(e))
                        .whenComplete((res, ex) -> {
                            if (ex == null) {
                                run.acked.add(orderId);
                            } else {
                                run.failed.incrementAndGet();
                            }
                        });
                run.sent.incrementAndGet();
                long due = t0 + i * perEventNanos;
                long wait = due - System.nanoTime();
                if (wait > 0) {
                    Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                }
            }
            modes.template(b.mode()).flush();
            // wait for the last callbacks (bounded by delivery.timeout.ms)
            long end = System.currentTimeMillis() + 35_000;
            while (run.acked.size() + run.failed.get() < b.count() && System.currentTimeMillis() < end) {
                Thread.sleep(50);
            }
        } catch (Exception ex) {
            run.error = ex.toString();
        }
        run.ms = (System.nanoTime() - t0) / 1_000_000;
        run.done = true;
    }

    @GetMapping("/bursts/{runId}")
    public Map<String, Object> status(@PathVariable String runId, @RequestParam(defaultValue = "false") boolean ids) {
        Run run = runs.get(runId);
        if (run == null) {
            return Map.of("error", "no such run");
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("runId", runId);
        out.put("done", run.done);
        out.put("sent", run.sent.get());
        out.put("acked", run.acked.size());
        out.put("failed", run.failed.get());
        out.put("ms", run.ms);
        out.put("error", run.error);
        if (ids) {
            out.put("ackedIds", run.acked);
        }
        return out;
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/ClusterController.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Who leads each partition, and how many replicas are in sync. */
@RestController
public class ClusterController {
    private final KafkaAdmin kafkaAdmin;

    public ClusterController(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @GetMapping("/cluster")
    public Map<String, Object> cluster() throws Exception {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            List<Integer> brokers = admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).stream()
                    .map(Node::id).sorted().toList();
            TopicDescription d = admin.describeTopics(List.of(Topics.ORDERS)).allTopicNames()
                    .get(5, TimeUnit.SECONDS).get(Topics.ORDERS);
            Map<Integer, Integer> leaders = new TreeMap<>();
            Map<Integer, Integer> isr = new TreeMap<>();
            int under = 0;
            for (TopicPartitionInfo p : d.partitions()) {
                leaders.put(p.partition(), p.leader() == null ? -1 : p.leader().id());
                isr.put(p.partition(), p.isr().size());
                if (p.isr().size() < p.replicas().size()) {
                    under++;
                }
            }
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("brokers", brokers);
            out.put("replicas", d.partitions().get(0).replicas().size());
            out.put("leaders", leaders);
            out.put("isr", isr);
            out.put("underReplicated", under);
            return out;
        }
    }
}
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** fulfillment-service: one member of the "fulfillment" consumer group. Run several to scale out. */
@RestController
@Profile("fulfillment")
public class FulfillmentService {
    private static final Logger log = LoggerFactory.getLogger(FulfillmentService.class);
    private final ObjectMapper json;
    private final LatencyStats latency = new LatencyStats();
    private final OrderTracker tracker = new OrderTracker();
    private final RunTracker runs = new RunTracker();
    private final RebalanceTracker rebalances;
    private final LagService lag;
    private final long processDelayMs;
    private final String instance;

    public FulfillmentService(ObjectMapper json, RebalanceTracker rebalances, LagService lag,
                              @Value("${app.process-delay-ms:1}") long processDelayMs,
                              @Value("${app.instance:f1}") String instance) {
        this.json = json;
        this.rebalances = rebalances;
        this.lag = lag;
        this.processDelayMs = processDelayMs;
        this.instance = instance;
    }

    @KafkaListener(topics = Topics.ORDERS, groupId = "fulfillment", concurrency = "${app.consumer.concurrency:1}")
    public void onEvent(ConsumerRecord<String, String> rec) throws Exception {
        OrderEvent e = json.readValue(rec.value(), OrderEvent.class);
        latency.record(e.orderId(), System.currentTimeMillis() - e.createdAtMs());
        runs.record(e.orderId());
        if (!tracker.record(e, rec.partition())) {
            log.warn("{} {} (seq {}) arrived out of order on partition {}", e.orderId(), e.type(), e.seq(), rec.partition());
        }
        if (processDelayMs > 0) {
            Thread.sleep(processDelayMs); // stands in for real work: reserve stock, print a label
        }
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("instance", instance);
        out.putAll(latency.snapshot());
        out.putAll(tracker.snapshot());
        out.putAll(rebalances.snapshot());
        return out;
    }

    @PostMapping("/stats/reset")
    public Map<String, Object> reset() {
        latency.reset();
        tracker.reset();
        return stats();
    }

    @GetMapping("/lag")
    public Map<String, Object> lag() throws Exception {
        Map<Integer, Long> byPartition = lag.lagByPartition("fulfillment", Topics.ORDERS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total", byPartition.values().stream().mapToLong(Long::longValue).sum());
        out.put("byPartition", byPartition);
        return out;
    }

    /** Day 4: what arrived for one burst (see BurstService). */
    @GetMapping("/runs/{runId}")
    public Map<String, Object> run(@PathVariable String runId, @RequestParam(defaultValue = "false") boolean ids) {
        return runs.snapshot(runId, ids);
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
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/LagService.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

/** Consumer lag = log-end offset minus the group's committed offset, per partition. */
@Component
public class LagService implements AutoCloseable {
    private final AdminClient admin;

    public LagService(KafkaAdmin kafkaAdmin) {
        this.admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    public Map<Integer, Long> lagByPartition(String group, String topic) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetSpec> latest = admin.describeTopics(java.util.List.of(topic)).allTopicNames()
                .get(5, TimeUnit.SECONDS).get(topic).partitions().stream()
                .collect(Collectors.toMap(p -> new TopicPartition(topic, p.partition()), p -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends = admin.listOffsets(latest).all()
                .get(5, TimeUnit.SECONDS);
        Map<Integer, Long> lag = new TreeMap<>();
        ends.forEach((tp, end) -> {
            OffsetAndMetadata c = committed.get(tp);
            lag.put(tp.partition(), end.offset() - (c == null ? 0 : c.offset()));
        });
        return lag;
    }

    @Override
    public void close() {
        admin.close();
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
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/ListenerConfig.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/** Attaches the rebalance tracker to the listener container that reads the orders topic. */
@Configuration
@Profile("fulfillment")
public class ListenerConfig {
    @Bean
    public RebalanceTracker rebalanceTracker() {
        return new RebalanceTracker();
    }

    @Bean
    public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> rebalanceCustomizer(
            RebalanceTracker tracker) {
        return container -> {
            String[] topics = container.getContainerProperties().getTopics();
            if (topics != null && java.util.Arrays.asList(topics).contains(Topics.ORDERS)) {
                container.getContainerProperties().setConsumerRebalanceListener(tracker);
            }
        };
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
    private final Set<String> disordered = new HashSet<>();
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
            disordered.add(e.orderId());
        }
        lastSeq.put(e.orderId(), Math.max(last, e.seq()));
        if (e.seq() == 3 && inOrder && !disordered.contains(e.orderId())) {
            completed++;
        }
        return inOrder;
    }

    public synchronized void reset() {
        lastSeq.clear();
        partitionsByOrder.clear();
        disordered.clear();
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
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/ProducerModes.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Three producer configurations, side by side, so the same burst can be replayed with each:
 *   acks1    acks=1, no idempotence: the leader alone confirms the write
 *   acksAll  acks=all, no idempotence: every in-sync replica has it, but retries can duplicate
 *   safe     acks=all + enable.idempotence=true: no loss, no duplicates on retry
 */
@Component
@Profile("orders")
public class ProducerModes {
    public static final String[] MODES = {"acks1", "acksAll", "safe"};
    private final Map<String, KafkaTemplate<String, String>> templates = new LinkedHashMap<>();

    public ProducerModes(@Value("${spring.kafka.bootstrap-servers}") String bootstrap) {
        for (String mode : MODES) {
            templates.put(mode, new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config(mode, bootstrap))));
        }
    }

    public static Map<String, Object> config(String mode, String bootstrap) {
        Map<String, Object> c = new HashMap<>();
        c.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        c.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        c.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        c.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        c.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30000);
        c.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        switch (mode) {
            case "acks1" -> {
                c.put(ProducerConfig.ACKS_CONFIG, "1");
                c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case "acksAll" -> {
                c.put(ProducerConfig.ACKS_CONFIG, "all");
                c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
            }
            case "safe" -> {
                c.put(ProducerConfig.ACKS_CONFIG, "all");
                c.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
                c.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            }
            default -> throw new IllegalArgumentException("unknown producer mode " + mode);
        }
        return c;
    }

    public KafkaTemplate<String, String> template(String mode) {
        KafkaTemplate<String, String> t = templates.get(mode);
        if (t == null) {
            throw new IllegalArgumentException("unknown producer mode " + mode);
        }
        return t;
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/RebalanceTracker.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;

/** Counts rebalances and remembers which partitions this instance owns right now. */
public class RebalanceTracker implements ConsumerAwareRebalanceListener {
    private static final Logger log = LoggerFactory.getLogger(RebalanceTracker.class);
    private final TreeSet<Integer> owned = new TreeSet<>();
    private int assignments;
    private int revocations;
    private int lost;

    @Override
    public synchronized void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        assignments++;
        partitions.forEach(tp -> owned.add(tp.partition()));
        log.info("rebalance #{}: assigned {} -> now own {}", assignments, partitions, owned);
    }

    @Override
    public synchronized void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        revocations++;
        partitions.forEach(tp -> owned.remove(tp.partition()));
    }

    @Override
    public synchronized void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        lost++;
        partitions.forEach(tp -> owned.remove(tp.partition()));
    }

    public synchronized Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("rebalances", assignments);
        out.put("revocations", revocations);
        out.put("partitionsLost", lost);
        out.put("ownedPartitions", new TreeSet<>(owned));
        return out;
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/main/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/main/java/com/shopstream/events/RunTracker.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Consumer side of Day 4: per run, how many events arrived and how many distinct orders. */
public class RunTracker {
    private final Map<String, AtomicInteger> received = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> unique = new ConcurrentHashMap<>();

    /** Burst order ids look like "safe-000123"; everything before the last '-' is the run id. */
    public static String runOf(String orderId) {
        int i = orderId.lastIndexOf('-');
        return i < 0 ? orderId : orderId.substring(0, i);
    }

    public void record(String orderId) {
        String run = runOf(orderId);
        received.computeIfAbsent(run, k -> new AtomicInteger()).incrementAndGet();
        unique.computeIfAbsent(run, k -> ConcurrentHashMap.newKeySet()).add(orderId);
    }

    public Map<String, Object> snapshot(String run, boolean ids) {
        Map<String, Object> out = new LinkedHashMap<>();
        int r = received.getOrDefault(run, new AtomicInteger()).get();
        Set<String> u = unique.getOrDefault(run, Set.of());
        out.put("runId", run);
        out.put("received", r);
        out.put("unique", u.size());
        out.put("duplicates", r - u.size());
        if (ids) {
            out.put("ids", u);
        }
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
    public NewTopic ordersTopic(@Value("${app.orders.partitions:6}") int partitions,
                                @Value("${app.orders.replicas:1}") int replicas) {
        // Day 4: three copies of every partition; a write needs two of them (min.insync.replicas=2).
        return TopicBuilder.name(ORDERS).partitions(partitions).replicas(replicas)
                .config("min.insync.replicas", replicas >= 3 ? "2" : "1").build();
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
# Day 3: cooperative rebalancing moves only the partitions that must move.
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
# Day 4: three brokers; the default template is the safe one.
app.orders.replicas=${ORDERS_REPLICAS:1}
spring.kafka.producer.acks=all
spring.kafka.producer.properties.enable.idempotence=true
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
mkdir -p "$PROJECT_DIR/src/test/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/ProducerModesTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

class ProducerModesTest {
    @Test
    void safeModeWaitsForAllReplicasAndIsIdempotent() {
        Map<String, Object> c = ProducerModes.config("safe", "localhost:19092");
        assertEquals("all", c.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, c.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    void unsafeModesSayWhatTheyGiveUp() {
        assertEquals("1", ProducerModes.config("acks1", "x").get(ProducerConfig.ACKS_CONFIG));
        assertEquals(false, ProducerModes.config("acksAll", "x").get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    void unknownModeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> ProducerModes.config("yolo", "x"));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/test/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/RebalanceTrackerTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class RebalanceTrackerTest {
    @Test
    void tracksOwnedPartitionsAcrossRebalances() {
        RebalanceTracker t = new RebalanceTracker();
        t.onPartitionsAssigned(null, List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1)));
        t.onPartitionsRevokedBeforeCommit(null, List.of(new TopicPartition("orders", 1)));
        t.onPartitionsAssigned(null, List.of(new TopicPartition("orders", 4)));
        assertEquals(2, t.snapshot().get("rebalances"));
        assertEquals(Set.of(0, 4), t.snapshot().get("ownedPartitions"));
        t.onPartitionsLost(null, List.of(new TopicPartition("orders", 0)));
        assertEquals(Set.of(4), t.snapshot().get("ownedPartitions"));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/src/test/java/com/shopstream/events"
cat > "$PROJECT_DIR/src/test/java/com/shopstream/events/RunTrackerTest.java" <<'SHOPSTREAM_EOF'
package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RunTrackerTest {
    @Test
    void countsDuplicatesPerRun() {
        RunTracker t = new RunTracker();
        t.record("safe-000001");
        t.record("safe-000002");
        t.record("safe-000002");
        t.record("acks1-000001");
        assertEquals(3, t.snapshot("safe", false).get("received"));
        assertEquals(2, t.snapshot("safe", false).get("unique"));
        assertEquals(1, t.snapshot("safe", false).get("duplicates"));
        assertEquals(1, t.snapshot("acks1", false).get("unique"));
        assertEquals("ORD", RunTracker.runOf("ORD-1001"));
    }
}
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/checks.py" <<'SHOPSTREAM_EOF'
"""Day 4 checks used by verify.sh."""
import time

import shop

for url in (shop.ORDERS_URL, shop.FULFILL_URL):
    if not shop.healthy(url):
        shop.fail("service not answering on " + url)
c = shop.cluster()
print(f"[VERIFY] brokers {c['brokers']}  replicas per partition {c['replicas']}  under-replicated {c['underReplicated']}")
if len(c["brokers"]) != 3 or c["replicas"] != 3:
    shop.fail("expected 3 brokers and 3 replicas of every partition")
if c["underReplicated"]:
    shop.wait_for(lambda: shop.cluster()["underReplicated"] == 0, timeout=60)
run_id = f"verify{int(time.time()) % 100000}"
shop.burst(run_id, "safe", 2000, 2000)
if not shop.wait_for(lambda: shop.burst_status(run_id)["done"], timeout=60):
    shop.fail("burst did not finish")
b = shop.burst_status(run_id)
shop.settle(run_id, quiet=2)
got = shop.consumed(run_id)
print(f"[VERIFY] safe producer: acknowledged {b['acked']}/2000, consumed {got['unique']} distinct, duplicates {got['duplicates']}")
if b["acked"] != 2000 or got["unique"] != 2000 or got["duplicates"] != 0:
    shop.fail("with acks=all and idempotence every acknowledged order must arrive exactly once")
SHOPSTREAM_EOF
mkdir -p "$PROJECT_DIR/tools"
cat > "$PROJECT_DIR/tools/demo.py" <<'SHOPSTREAM_EOF'
"""Day 4 demo: kill the leader broker in the middle of a burst, once per producer mode."""
import time

import shop

COUNT, RATE = 20000, 5000
rows = []
for mode in ("acks1", "acksAll", "safe"):
    c = shop.cluster()
    if c["underReplicated"]:
        shop.wait_for(lambda: shop.cluster()["underReplicated"] == 0, timeout=90)
    victim = shop.cluster()["leaders"]["0"]
    run_id = f"{mode}{int(time.time()) % 100000}"
    print(f"\n[{mode}] {COUNT} orders at {RATE}/s; kill -9 broker {victim} (leader of partition 0) after 1.5 s")
    shop.burst(run_id, mode, COUNT, RATE)
    time.sleep(1.5)
    shop.kill_broker(victim)
    t_kill = time.time()
    shop.wait_for(lambda: shop.burst_status(run_id)["done"], timeout=120, every=0.5)
    b = shop.burst_status(run_id, ids=True)
    print(f"  producer: sent {b['sent']}  acknowledged {b['acked']}  failed {b['failed']}  ({b['ms']} ms)")
    shop.start_broker(victim)
    shop.wait_for(lambda: shop.cluster()["underReplicated"] == 0, timeout=120, every=1)
    print(f"  broker {victim} back and all replicas in sync {time.time() - t_kill:.1f} s after the kill")
    shop.settle(run_id)
    got = shop.consumed(run_id, ids=True)
    lost = len(set(b["ackedIds"]) - set(got["ids"]))
    print(f"  consumer: received {got['received']}  distinct orders {got['unique']}  "
          f"LOST (acknowledged, never delivered) {lost}  DUPLICATES {got['duplicates']}")
    rows.append((mode, b["acked"], b["failed"], lost, got["duplicates"]))

print("\nmode      acked   failed   lost   duplicates")
for m, a, f, l, d in rows:
    print(f"{m:8} {a:6}  {f:6}  {l:5}  {d:9}")
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
shop.start_fulfillment(["f1"], ["--app.consumer.concurrency=3", "--app.process-delay-ms=0"])
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
export KAFKA_BOOTSTRAP="localhost:19092,localhost:19192,localhost:19292"
python3 "$ROOT/shopstream-events/tools/checks.py"
echo "PASS  Lesson 4 complete. Your completion code: SHOP2-ACKS-6Q4Z"
VERIFY_EOF
chmod +x "$ROOT/verify.sh"

say "Verify"
bash "$ROOT/verify.sh"

say "Demo"
python3 -u "$PROJECT_DIR/tools/demo.py"

say "Done"
echo "order-service:       http://localhost:$ORDERS_PORT   (POST /orders)"
echo "fulfillment-service: http://localhost:$FULFILLMENT_PORT   (GET /stats)"
echo "Run bash verify.sh for your completion code, bash stop.sh when you are finished."

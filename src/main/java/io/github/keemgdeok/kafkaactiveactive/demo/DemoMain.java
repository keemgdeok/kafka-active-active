package io.github.keemgdeok.kafkaactiveactive.demo;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import io.github.keemgdeok.kafkaactiveactive.consume.ActiveKafkaConsumer;
import io.github.keemgdeok.kafkaactiveactive.consume.CommitResult;
import io.github.keemgdeok.kafkaactiveactive.consume.ConsumerSettings;
import io.github.keemgdeok.kafkaactiveactive.consume.GroupTransition;
import io.github.keemgdeok.kafkaactiveactive.environment.KafkaTopologyVerifier;
import io.github.keemgdeok.kafkaactiveactive.publish.DualKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.EventDataset;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

/** Runnable example only. Docker orchestration is optional and never used by SDK classes. */
public final class DemoMain {
    private DemoMain() {}
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("--help")) {
            System.out.println("--bootstrap-a host:port --bootstrap-b host:port --count 20 --seed 0 [--compose-project kaa-demo-xxxxxxxx]");
            return;
        }
        Map<String, String> options = options(args);
        String bootstrapA = options.getOrDefault("bootstrap-a", "localhost:19092");
        String bootstrapB = options.getOrDefault("bootstrap-b", "localhost:29092");
        int count = Integer.parseInt(options.getOrDefault("count", "20"));
        long seed = Long.parseLong(options.getOrDefault("seed", "0"));
        String project = options.get("compose-project");
        if (project != null && (!project.matches("kaa-demo-[0-9a-f]{8}") || count < 6)) {
            throw new IllegalArgumentException("fault demo needs a dedicated kaa-demo-xxxxxxxx project and count >= 6");
        }
        String suffix = UUID.randomUUID().toString();
        String topic = "kaa-demo-" + suffix;
        var settings = new ConsumerSettings(topic, "kaa-demo-" + suffix, Duration.ofSeconds(5), 100,
                100_000, 64L * 1024 * 1024, 900_000);
        var a = Map.<String, Object>of("bootstrap.servers", bootstrapA);
        var b = Map.<String, Object>of("bootstrap.servers", bootstrapB);
        var topology = KafkaTopologyVerifier.verify(bootstrapA, bootstrapB, Duration.ofSeconds(10));
        createTopic(a, topic);
        createTopic(b, topic);
        var events = EventDataset.generate(count, seed, false);
        var expected = events.stream().collect(java.util.stream.Collectors.toMap(event -> event.eventId(), event -> event));
        var running = new AtomicBoolean(true);
        var stopped = new AtomicBoolean(false);
        var ready = new AtomicBoolean(false);
        var calls = new ConcurrentHashMap<String, AtomicInteger>();
        var completed = ConcurrentHashMap.<String>newKeySet();
        boolean brokerStopped = false;
        try (var evidence = new JsonlEvidence(Path.of("build", "reports", "demo", "events.jsonl"));
             var executor = Executors.newSingleThreadExecutor()) {
            evidence.record("start", Map.of("topic", topic, "group", settings.groupId(), "seed", seed,
                    "cluster_a", topology.clusterAId(), "cluster_b", topology.clusterBId()));
            Future<?> worker = executor.submit(() -> {
                try (var consumer = new ActiveKafkaConsumer(settings, b, a)) {
                    while (running.get()) {
                        var batch = consumer.poll(Duration.ofMillis(100));
                        if (!consumer.status().assignedPartitions().isEmpty()) { ready.set(true); }
                        if (batch.isEmpty()) { continue; }
                        for (var delivery : batch.records()) {
                            String id = delivery.event().eventId();
                            if (!delivery.event().equals(expected.get(id))) {
                                throw new IllegalStateException("handler received unexpected key or payload");
                            }
                            int invocation = calls.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet();
                            evidence.record("handler", Map.of("id", id, "invocation", invocation));
                        }
                        var result = consumer.commit(batch.processed()).toCompletableFuture().join();
                        if (result.status() == CommitResult.Status.UNKNOWN) { throw new IllegalStateException("uncertain consumer completion"); }
                        completed.addAll(result.locallyCompletedIds());
                        evidence.record("consumer_commit", Map.of("status", result.status(), "local_ids", result.locallyCompletedIds().size()));
                    }
                } finally { stopped.set(true); }
            });
            try {
                await(ready::get, worker);
                var publishConfig = new PublishConfig(topic, Duration.ofSeconds(2), 128, 900_000, Duration.ofSeconds(3));
                try (var publisher = DualKafkaPublisher.open(publishConfig, a, b)) {
                    for (int i = 0; i < events.size(); i++) {
                        if (project != null && i == count / 3) {
                            compose(project, "stop", "kafka-a");
                            brokerStopped = true;
                            evidence.record("fault", Map.of("cluster", "A", "action", "process_stop"));
                        }
                        if (project != null && i == 2 * count / 3) {
                            compose(project, "up", "-d", "--wait", "kafka-a");
                            brokerStopped = false;
                            evidence.record("recovery", Map.of("cluster", "A", "action", "broker_started"));
                        }
                        var result = publisher.send(events.get(i)).toCompletableFuture().get(5, TimeUnit.SECONDS);
                        evidence.record("publish", Map.of("id", result.eventId(), "partition", result.partition(), "status", result.status(),
                                "a_offset", result.clusterA().offset() == null ? "unconfirmed" : result.clusterA().offset(),
                                "b_offset", result.clusterB().offset() == null ? "unconfirmed" : result.clusterB().offset()));
                        if (!result.successful()) { throw new IllegalStateException("input was not accepted: " + result); }
                    }
                }
                await(() -> completed.size() == count, worker);
            } finally {
                running.set(false);
                if (brokerStopped) { compose(project, "up", "-d", "--wait", "kafka-a"); }
                worker.get(30, TimeUnit.SECONDS);
            }
            if (calls.size() != count || calls.values().stream().anyMatch(value -> value.get() != 1)) {
                throw new IllegalStateException("handler delivery mismatch");
            }
            // B was the active group. Closed worker state is the stop evidence for transition to A.
            var prepared = new GroupTransition(settings, b, a, stopped::get).prepare();
            evidence.record("repair", Map.of("copied_to_a", prepared.dataReport().copiedToB(),
                    "completion_copies_to_a", prepared.completionReport().copiedToB()));
            try (var target = prepared.startTarget()) {
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (target.status().filteredRecords() < count && System.nanoTime() < deadline) {
                    if (!target.poll(Duration.ofMillis(100)).isEmpty()) {
                        throw new IllegalStateException("completed event reached target handler");
                    }
                }
                if (target.status().filteredRecords() < count) { throw new IllegalStateException("target replay incomplete"); }
                evidence.record("transition", Map.of("target", "A", "filtered_records", target.status().filteredRecords(), "handler_calls", 0));
            }
            for (var entry : Map.of("A", a, "B", b).entrySet()) {
                var report = EventAudit.read(entry.getValue(), List.of(topic), events);
                report.requireComplete();
                evidence.record("audit", Map.of("cluster", entry.getKey(), "unique_events", report.uniqueEvents(),
                        "physical_records", report.physicalRecords(), "missing", report.missingIds().size(),
                        "payload_conflicts", report.payloadConflicts(), "key_order_inversions", report.keyOrderInversions()));
            }
            evidence.record("success", Map.of("accepted_events", count, "handler_calls", calls.size(), "completed_redeliveries", 0));
        }
    }
    private static void await(BooleanSupplier condition, Future<?> worker) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            if (worker.isDone()) { worker.get(); throw new IllegalStateException("consumer stopped before verification completed"); }
            LockSupport.parkNanos(Duration.ofMillis(20).toNanos());
        }
        if (!condition.getAsBoolean()) { throw new IllegalStateException("demo condition deadline elapsed"); }
    }
    private static void createTopic(Map<String, Object> properties, String topic) throws Exception {
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                    "retention.ms", "-1", "retention.bytes", "-1", "cleanup.policy", "delete"))))
                    .all().get(15, TimeUnit.SECONDS);
        }
    }
    private static void compose(String project, String... args) throws Exception {
        var command = new ArrayList<>(List.of("docker", "compose", "--project-name", project, "-f", "infra/compose.yaml"));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).inheritIO().start();
        if (!process.waitFor(90, TimeUnit.SECONDS) || process.exitValue() != 0) {
            process.destroyForcibly();
            throw new IllegalStateException("demo Docker command failed");
        }
    }
    private static Map<String, String> options(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 >= args.length || !List.of("--bootstrap-a", "--bootstrap-b", "--count", "--seed", "--compose-project").contains(args[i])) {
                throw new IllegalArgumentException("invalid demo arguments; use --help");
            }
            values.put(args[i].substring(2), args[i + 1]);
        }
        return values;
    }
}

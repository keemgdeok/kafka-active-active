import io.github.keemgdeok.kafkaactiveactive.consume.ActiveKafkaConsumer;
import io.github.keemgdeok.kafkaactiveactive.consume.CommitResult;
import io.github.keemgdeok.kafkaactiveactive.consume.CompletionReplicator;
import io.github.keemgdeok.kafkaactiveactive.consume.ConsumerSettings;
import io.github.keemgdeok.kafkaactiveactive.consume.GroupTransition;
import io.github.keemgdeok.kafkaactiveactive.environment.KafkaTopologyVerifier;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishResult;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairException;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairService;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class ReleasedSdkCheck {
    private static final Map<String, Object> A = Map.of("bootstrap.servers", "localhost:19092");
    private static final Map<String, Object> B = Map.of("bootstrap.servers", "localhost:29092");
    private static final Duration POLL = Duration.ofMillis(100);
    private static final int WINDOW = 16;
    private static final int OUTAGE_EVENTS = 8;
    private static final int PAYLOAD_BYTES = 256;

    public static void main(String[] args) throws Exception {
        String version = args[0];
        int count = Integer.parseInt(args[1]);
        String project = args[2];
        String topic = "verification-events";
        var settings = ConsumerSettings.defaults(topic, "verification-consumer");
        var healthy = events("order", count);
        var outage = events("outage", OUTAGE_EVENTS);
        var probe = event("probe", 1);
        var afterSwitch = event("after-switch", 0);
        check(probe.partition() == 1 && afterSwitch.partition() == 0, "fixture partition mismatch");
        var expected = new ArrayList<>(List.of(probe));
        expected.addAll(healthy);
        expected.addAll(outage);
        var sourceEvents = expected.stream().collect(Collectors.toMap(Event::eventId, Function.identity()));

        var state = KafkaTopologyVerifier.verify("localhost:19092", "localhost:29092", Duration.ofSeconds(10));
        createTopic(A, topic);
        createTopic(B, topic);
        try (var evidence = new JsonlEvidence(Path.of("events.jsonl"));
             var replication = new ReplicationService(ReplicationConfig.defaults(topic), A, B);
             var publisher = RoutedKafkaPublisher.open(PublishConfig.defaults(topic), A, B)) {
            evidence.record("environment", Map.of("sdk", version, "cluster_a", state.clusterAId(),
                    "cluster_b", state.clusterBId(), "healthy_events", count, "outage_events", OUTAGE_EVENTS,
                    "payload_bytes", PAYLOAD_BYTES, "publish_window", WINDOW));
            replication.start();
            await("publisher and replication ready", Duration.ofSeconds(30), () ->
                    publisher.status().clusterAConfirmationReady() && publisher.status().clusterBConfirmationReady()
                            && replication.status().aToB().state() == ReplicationService.State.RUNNING
                            && replication.status().bToA().state() == ReplicationService.State.RUNNING);

            var completedOnB = new ConcurrentHashMap<String, CommitResult.Status>();
            var ready = new CompletableFuture<Void>();
            var stopSource = new AtomicBoolean();
            var sourceResult = new CompletableFuture<Void>();
            Thread source = Thread.ofPlatform().daemon().start(() -> {
                try (var consumer = new ActiveKafkaConsumer(settings, B, A)) {
                    while (!stopSource.get()) {
                        var batch = consumer.poll(POLL);
                        if (consumer.status().assignedPartitions().size() == 2) ready.complete(null);
                        if (batch.isEmpty()) continue;
                        for (var delivery : batch.records()) {
                            var event = delivery.event();
                            check(event.equals(sourceEvents.get(event.eventId())), "unexpected event on B");
                        }
                        var commit = consumer.commit(batch.processed()).toCompletableFuture().join();
                        check(commit.status() != CommitResult.Status.UNKNOWN, "consumer commit is uncertain");
                        for (var delivery : batch.records()) {
                            check(completedOnB.putIfAbsent(delivery.event().eventId(), commit.status()) == null,
                                    "B delivered an event twice");
                            check(commit.locallyCompletedIds().contains(delivery.event().eventId()),
                                    "delivery missing from local commit");
                        }
                    }
                } catch (Exception e) {
                    ready.completeExceptionally(e);
                    sourceResult.completeExceptionally(e);
                    return;
                }
                sourceResult.complete(null);
            });

            long recoveredAt;
            try {
                ready.get(40, TimeUnit.SECONDS);
                long probeAt = System.nanoTime();
                var probeResult = publisher.send(probe).toCompletableFuture();
                await("B to A probe copied", Duration.ofSeconds(15),
                        () -> replication.status().bToA().replicatedRecords() == 1);
                evidence.record("replication_probe", Map.of("direction", "B_to_A", "send_to_observed_copy_ms",
                        elapsedMs(probeAt)));
                check(probeResult.get(6, TimeUnit.SECONDS).status() == PublishResult.Status.CONFIRMED,
                        "warmup event failed to confirm both copies");
                publish("healthy", healthy, publisher, evidence, false);
                awaitSourceCount(completedOnB, 1 + count, sourceResult);
                await("healthy replication caught up", Duration.ofSeconds(60), () ->
                        replication.status().bToA().replicatedRecords() >= 1 + count / 2
                                && replication.status().aToB().replicatedRecords() >= (count + 1) / 2);
                var before = EventAudit.read(A, List.of(topic), expected.subList(0, 1 + count));
                before.requireComplete();
                long copiesBeforeOutage = replication.status().bToA().replicatedRecords();

                compose(project, "stop", "kafka-a");
                long downAt = System.nanoTime();
                evidence.record("broker_stopped", Map.of("broker", "A"));
                publish("outage", outage, publisher, evidence, true);
                awaitSourceCount(completedOnB, expected.size(), sourceResult);
                check(outage.stream().allMatch(event ->
                                completedOnB.get(event.eventId()) == CommitResult.Status.LOCAL_ONLY),
                        "outage events must complete on B while A is stopped");
                evidence.record("outage_consumption", Map.of("committed_on_b", OUTAGE_EVENTS,
                        "commit_status", CommitResult.Status.LOCAL_ONLY, "duration_ms", elapsedMs(downAt)));

                long restartAt = System.nanoTime();
                compose(project, "up", "-d", "--wait", "kafka-a");
                recoveredAt = System.nanoTime();
                await("outage copies replayed on A", Duration.ofSeconds(60), () ->
                        replication.status().bToA().replicatedRecords() >= copiesBeforeOutage + OUTAGE_EVENTS);
                evidence.record("replication_recovery", Map.of("outage_events", OUTAGE_EVENTS,
                        "restart_to_copies_ms", elapsedMs(restartAt),
                        "healthy_to_observed_copies_ms", elapsedMs(recoveredAt)));
            } finally {
                stopSource.set(true);
                sourceResult.get(40, TimeUnit.SECONDS);
                source.join();
            }

            long prepareAt = System.nanoTime();
            var prepared = new GroupTransition(settings, B, A, () -> !source.isAlive()).prepare();
            var data = prepared.dataReport();
            check(prepared.completionReport().copiedToB() >= OUTAGE_EVENTS
                            && prepared.completionReport().completedIds() == expected.size(),
                    "completion repair did not recover the outage history");
            evidence.record("prepare", Map.of("duration_ms", elapsedMs(prepareAt),
                    "data_scan_ms", Duration.between(data.startedAt(), data.completedAt()).toMillis(),
                    "indexed_records", data.indexedRecords(), "estimated_index_bytes", data.estimatedIndexBytes(),
                    "copied_to_kafka_a", data.copiedToB(),
                    "completion_copied_to_kafka_a", prepared.completionReport().copiedToB()));

            long switchAt = System.nanoTime();
            try (var target = prepared.startTarget()) {
                await("target replay", Duration.ofSeconds(40), () -> {
                    check(target.poll(POLL).isEmpty(), "completed event reached target handler");
                    return target.status().filteredRecords() >= expected.size();
                });
                check(target.status().deliveredEvents() == 0, "target redelivered completed events");
                evidence.record("transition", Map.of("duration_ms", elapsedMs(switchAt),
                        "recovery_to_consumer_ms", elapsedMs(recoveredAt),
                        "completed_redeliveries", 0, "filtered_records", target.status().filteredRecords()));
                long copiesBeforeSend = replication.status().aToB().replicatedRecords();
                publish("after_switch", List.of(afterSwitch), publisher, evidence, false);
                await("new delivery on A", Duration.ofSeconds(30), () -> {
                    var batch = target.poll(POLL);
                    if (batch.isEmpty()) return false;
                    check(batch.records().size() == 1 && batch.records().getFirst().event().equals(afterSwitch),
                            "target did not deliver only the new event");
                    var commit = target.commit(batch.processed()).toCompletableFuture().join();
                    check(commit.status() == CommitResult.Status.CONFIRMED
                                    && commit.peerConfirmedIds().contains(afterSwitch.eventId()),
                            "target did not commit the new event on both clusters");
                    return true;
                });
                await("new event copied to B", Duration.ofSeconds(30), () ->
                        replication.status().aToB().replicatedRecords() > copiesBeforeSend);
            }

            expected.add(afterSwitch);
            audit("A", A, topic, expected, evidence);
            audit("B", B, topic, expected, evidence);
            checkLimits(settings);
            evidence.record("success", Map.of("accepted_events", expected.size(),
                    "published_version", version, "configured_limit_checks", 2));
        }
    }

    private static List<Event> events(String prefix, int count) {
        return IntStream.range(0, count).mapToObj(i -> event(prefix, i)).toList();
    }

    private static Event event(String prefix, int index) {
        String key = index % 2 == 0 ? "kafka" : "hello";
        return new Event(prefix + "-" + index, key,
                ByteBuffer.allocate(PAYLOAD_BYTES).putInt(prefix.hashCode()).putInt(index).array());
    }

    private record Measurement(PublishResult result, long nanos) {}

    private static void publish(String phase, List<Event> events, RoutedKafkaPublisher publisher,
                                JsonlEvidence evidence, boolean outage) throws Exception {
        var samples = new ArrayList<Measurement>();
        long start = System.nanoTime();
        for (int from = 0; from < events.size(); from += WINDOW) {
            var pending = new ArrayList<CompletableFuture<Measurement>>();
            for (Event event : events.subList(from, Math.min(from + WINDOW, events.size()))) {
                long sentAt = System.nanoTime();
                pending.add(publisher.send(event)
                        .thenApply(result -> new Measurement(result, System.nanoTime() - sentAt)).toCompletableFuture());
            }
            for (var future : pending) samples.add(future.get(7, TimeUnit.SECONDS));
        }
        long duration = elapsedMs(start);
        var statuses = new EnumMap<PublishResult.Status, Integer>(PublishResult.Status.class);
        for (var sample : samples) {
            var result = sample.result();
            check(result.successful(), "publication failed: " + result);
            if (outage) {
                check(result.status() == PublishResult.Status.ACCEPTED_DEGRADED
                                && result.clusterB().evidence() == PublishResult.Evidence.DIRECT_RAW_ACK,
                        "outage event did not land on B: " + result);
            }
            statuses.merge(result.status(), 1, Integer::sum);
        }
        var latencies = samples.stream().map(Measurement::nanos).sorted().toList();
        evidence.record("publish", Map.of("phase", phase, "count", events.size(),
                "confirmed", statuses.getOrDefault(PublishResult.Status.CONFIRMED, 0),
                "accepted_degraded", statuses.getOrDefault(PublishResult.Status.ACCEPTED_DEGRADED, 0),
                "duration_ms", duration, "events_per_second", events.size() * 1_000.0 / Math.max(1, duration),
                "p50_ms", percentile(latencies, 0.50), "p95_ms", percentile(latencies, 0.95),
                "p99_ms", percentile(latencies, 0.99)));
    }

    private static double percentile(List<Long> nanos, double quantile) {
        return nanos.get((int) Math.ceil(nanos.size() * quantile) - 1) / 1_000_000.0;
    }

    private static void audit(String broker, Map<String, Object> cluster, String topic,
                              List<Event> expected, JsonlEvidence evidence) throws Exception {
        var report = EventAudit.read(cluster, List.of(topic), expected);
        report.requireComplete();
        evidence.record("audit", Map.of("broker", broker, "unique_events", report.uniqueEvents(),
                "physical_records", report.physicalRecords(), "missing", report.missingIds().size(),
                "unexpected", report.unexpectedIds().size(), "payload_conflicts", report.payloadConflicts()));
    }

    private static void checkLimits(ConsumerSettings settings) {
        var defaults = RepairConfig.defaults(settings.topic());
        try (var repair = new RepairService(new RepairConfig(defaults.topic(), defaults.runTimeout(),
                defaults.interval(), 4, defaults.maxIndexBytes(), defaults.maxEventBytes()), A, B)) {
            repair.runOnce();
            throw new AssertionError("repair accepted an undersized index");
        } catch (RepairException e) {
            check(e.reason() == RepairException.Reason.INDEX_LIMIT, e.toString());
        }
        var small = new ConsumerSettings(settings.topic(), settings.groupId(), settings.operationTimeout(),
                settings.maxBatchRecords(), 4, settings.maxCompletedBytes(), settings.maxEventBytes());
        try {
            new CompletionReplicator(small, A, B).runOnce();
            throw new AssertionError("completion replay accepted an undersized index");
        } catch (IllegalStateException e) {
            check("completion index limit exceeded".equals(e.getMessage()), e.toString());
        }
    }

    private static void awaitSourceCount(Map<String, CommitResult.Status> completed, int count,
                                         CompletableFuture<Void> source) throws Exception {
        await("commit on B", Duration.ofSeconds(120), () -> completed.size() >= count || source.isDone());
        if (source.isDone()) source.get();
        check(completed.size() == count, "unexpected source completion count");
    }

    private static void await(String action, Duration timeout, BooleanSupplier finished) throws InterruptedException {
        long start = System.nanoTime();
        while (!finished.getAsBoolean()) {
            check(System.nanoTime() - start < timeout.toNanos(), "timed out waiting for " + action);
            Thread.sleep(25);
        }
    }

    private static long elapsedMs(long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }

    private static void createTopic(Map<String, Object> properties, String name) throws Exception {
        try (var admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(name, 2, (short) 1).configs(Map.of(
                    "retention.ms", "-1", "retention.bytes", "-1", "cleanup.policy", "delete"))))
                    .all().get(15, TimeUnit.SECONDS);
        }
    }

    private static void compose(String project, String... args) throws Exception {
        var command = new ArrayList<>(List.of("docker", "compose", "--project-name", project,
                "-f", System.getenv("KAA_COMPOSE_FILE")));
        command.addAll(List.of(args));
        var process = new ProcessBuilder(command).inheritIO().start();
        if (!process.waitFor(90, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("Docker Compose command timed out: " + command);
        }
        check(process.exitValue() == 0, "Docker Compose command failed: " + command);
    }
}

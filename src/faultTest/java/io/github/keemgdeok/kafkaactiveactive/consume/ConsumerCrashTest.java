package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.support.CrashProcess;
import io.github.keemgdeok.kafkaactiveactive.support.FaultSeeds;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("try")
class ConsumerCrashTest {
    private static final KafkaPair KAFKA = new KafkaPair("fault-consumer-brokers");
    @BeforeAll static void start() { KAFKA.start(); }
    @AfterAll static void stop() throws Exception { KAFKA.close(); }
    static Stream<Arguments> cases() {
        return FaultSeeds.values().boxed().flatMap(seed -> Stream.of("AFTER_HANDLER", "MARKERS_SENT", "OFFSETS_STAGED", "LOCAL_COMMITTED")
                .map(point -> Arguments.of(seed, point)));
    }
    @ParameterizedTest(name = "seed={0} crash={1}") @MethodSource("cases")
    void abruptCrashPreservesOffsetCompletionAtomicityAndSafeTransition(int seed, String point) throws Exception {
        String topic = "crash-" + UUID.randomUUID();
        KAFKA.createTopic(topic);
        var settings = new ConsumerSettings(topic, "group-" + UUID.randomUUID(), Duration.ofSeconds(5), 100, 1_000, 1_000_000, 1_024);
        var events = List.of(ConsumerCrashChild.input(seed));
        try (var evidence = new JsonlEvidence(Path.of("build/reports/faultTest", "consumer-" + seed + "-" + point + ".jsonl"))) {
            evidence.record("scenario", Map.of("seed", seed, "topic", topic, "group", settings.groupId(), "fault_point", point,
                    "partition", events.getFirst().partition(), "operation_timeout_ms", settings.operationTimeout().toMillis()));
            try (var replication = ReplicationFixture.start(topic, KAFKA.aProperties(), KAFKA.bProperties());
                 var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(PublishConfig.defaults(topic), KAFKA.aProperties(), KAFKA.bProperties()))) {
                var result = publisher.send(events.getFirst()).toCompletableFuture().get(10, TimeUnit.SECONDS);
                assertTrue(result.successful());
                evidence.record("attempt", Map.of("seed", seed, "event_id", result.eventId(), "status", result.status()));
            }
            CrashProcess.run(ConsumerCrashChild.class.getName(), "consumer-" + seed + "-" + point,
                    topic, settings.groupId(), KAFKA.a.getBootstrapServers(), KAFKA.b.getBootstrapServers(), point, Integer.toString(seed));
            awaitEmpty(settings);
            boolean completed = point.equals("LOCAL_COMMITTED");
            try (Admin admin = Admin.create(KAFKA.aProperties())) {
                var offsets = admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var offset = offsets.get(new org.apache.kafka.common.TopicPartition(topic, events.getFirst().partition()));
                assertEquals(completed ? 1L : -1L, offset == null ? -1L : offset.offset());
            }
            // CrashProcess returns only after actual OS process exit. Empty native membership alone is not our stop proof.
            var transition = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> true).prepare();
            assertEquals(completed ? 1 : 0, transition.completionReport().completedIds());
            int calls = 0;
            try (var target = transition.startTarget()) {
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (System.nanoTime() < deadline) {
                    var batch = target.poll(Duration.ofMillis(100));
                    for (var delivery : batch.records()) { assertEquals(events.getFirst(), delivery.event()); calls++; }
                    if (!batch.isEmpty()) { assertEquals(CommitResult.Status.CONFIRMED, target.commit(batch.processed()).toCompletableFuture().join().status()); }
                    if (target.status().completedIds() == 1 && (calls > 0 || target.status().filteredRecords() > 0)) { break; }
                }
                assertEquals(1, target.status().completedIds());
            }
            assertEquals(completed ? 0 : 1, calls);
            try (var a = new CompletionLedger(settings, KAFKA.aProperties()); var b = new CompletionLedger(settings, KAFKA.bProperties())) {
                assertEquals(1, a.replay(a.snapshot()).size()); assertEquals(1, b.replay(b.snapshot()).size());
            }
            EventAudit.read(KAFKA.aProperties(), List.of(topic), events).requireComplete();
            EventAudit.read(KAFKA.bProperties(), List.of(topic), events).requireComplete();
            evidence.record("verified", Map.of("seed", seed, "fault_point", point, "locally_completed_before_crash", completed,
                    "target_handler_calls", calls, "completed_redeliveries", 0, "completed_ids_both", 1));
        }
    }
    private static void awaitEmpty(ConsumerSettings settings) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        try (Admin admin = Admin.create(KAFKA.aProperties())) {
            while (System.nanoTime() < deadline) {
                try {
                    if (admin.describeConsumerGroups(List.of(settings.groupId())).all().get(5, TimeUnit.SECONDS)
                            .get(settings.groupId()).members().isEmpty()) { return; }
                } catch (java.util.concurrent.ExecutionException failure) {
                    if (!(failure.getCause() instanceof GroupIdNotFoundException)) { throw failure; }
                    return;
                }
                LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
            }
        }
        throw new AssertionError("crashed source group did not become empty");
    }
}

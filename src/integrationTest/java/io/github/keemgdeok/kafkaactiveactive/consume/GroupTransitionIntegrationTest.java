package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import io.github.keemgdeok.kafkaactiveactive.demo.DemoMain;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairException;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.ManagedConsumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@SuppressWarnings("try")
class GroupTransitionIntegrationTest {
    private static final KafkaPair KAFKA = new KafkaPair("group-brokers");
    @BeforeAll static void start() { KAFKA.start(); }
    @AfterAll static void stop() throws Exception { KAFKA.close(); }

    @Test void differentLogicalGroupsConsumeBothKafkasConcurrentlyWithoutSharingCompletionState() throws Exception {
        var aSettings = scenario();
        var bSettings = new ConsumerSettings(aSettings.topic(), "other-" + UUID.randomUUID(), aSettings.operationTimeout(),
                100, 1_000, 1_000_000, 1_024);
        var aCalls = new ConcurrentHashMap<String, AtomicInteger>();
        var bCalls = new ConcurrentHashMap<String, AtomicInteger>();
        try (var a = new ManagedConsumer(aSettings, KAFKA.aProperties(), KAFKA.bProperties(), aCalls, false);
             var b = new ManagedConsumer(bSettings, KAFKA.bProperties(), KAFKA.aProperties(), bCalls, false)) {
            publish(aSettings, 4, "both-groups-");
            await(() -> a.committedIds().size() == 4 && b.committedIds().size() == 4);
            assertEquals(aCalls.keySet(), bCalls.keySet());
            assertTrue(aCalls.values().stream().allMatch(count -> count.get() == 1));
            assertTrue(bCalls.values().stream().allMatch(count -> count.get() == 1));
        }
    }

    @Test void nativeRebalanceAssignsTwoPartitionsAndParksExcessInstances() throws Exception {
        var settings = scenario();
        var calls = new ConcurrentHashMap<String, AtomicInteger>();
        try (var one = runner(settings, calls); var two = runner(settings, calls)) {
            await(() -> stableMembers(settings, 2) && one.assignment().size() == 1 && two.assignment().size() == 1);
            assertTrue(Collections.disjoint(one.assignment(), two.assignment()));
            try (var three = runner(settings, calls)) {
                await(() -> stableMembers(settings, 3)
                        && one.assignment().size() + two.assignment().size() + three.assignment().size() == 2);
                assertEquals(1, List.of(one, two, three).stream().filter(r -> r.assignment().isEmpty()).count());
                publish(settings, 20, "initial-");
                await(() -> calls.size() == 20 && Stream.of(one, two, three)
                        .flatMap(r -> r.committedIds().stream()).distinct().count() == 20);
            }
            await(() -> stableMembers(settings, 2) && one.assignment().size() == 1 && two.assignment().size() == 1);
            publish(settings, 20, "after-exit-");
            await(() -> calls.size() == 40 && Stream.of(one, two)
                    .flatMap(r -> r.committedIds().stream()).filter(id -> id.startsWith("after-exit-")).distinct().count() == 20);
        }
        assertTrue(calls.values().stream().allMatch(count -> count.get() == 1));
    }

    @Test void oldGroupMemberCannotCommitAfterItsPollLeaseExpiresAndNewOwnerCompletes() throws Exception {
        var settings = scenario();
        var calls = new ConcurrentHashMap<String, AtomicInteger>();
        var properties = new HashMap<>(KAFKA.aProperties());
        properties.put("max.poll.interval.ms", 10_000);
        try (var old = new ManagedConsumer(settings, properties, KAFKA.bProperties(), calls, true)) {
            publish(settings, 1, "stale-");
            old.awaitDelivery();
            try (var replacement = runner(settings, calls)) {
                await(() -> calls.get("stale-0").get() == 2 && replacement.lastCommit() != null);
                old.release();
                await(() -> old.lastCommit() != null);
                assertEquals(CommitResult.Status.UNKNOWN, old.lastCommit().status());
            }
        }
        try (var ledger = new CompletionLedger(settings, KAFKA.aProperties())) {
            assertEquals(1, ledger.replay(ledger.snapshot()).size());
        }
    }

    @Test void transitionRequiresProcessStopAndEmptyGroupsThenFiltersHistoryOnTarget() throws Exception {
        var settings = scenario();
        var calls = new ConcurrentHashMap<String, AtomicInteger>();
        var old = runner(settings, calls);
        try {
            publish(settings, 4, "done-");
            await(() -> calls.size() == 4 && old.committedIds().size() == 4);
            var unchecked = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> false);
            assertThrows(IllegalStateException.class, unchecked::prepare);
            var falselyStopped = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> true);
            assertThrows(IllegalStateException.class, falselyStopped::prepare);
        } finally { old.close(); }
        var prepared = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), old::stopped).prepare();
        assertEquals(4, prepared.completionReport().completedIds());
        try (var target = prepared.startTarget()) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (target.status().filteredRecords() < 4 && System.nanoTime() < deadline) {
                assertTrue(target.poll(Duration.ofMillis(100)).isEmpty());
            }
            assertTrue(target.status().filteredRecords() >= 4);
            assertEquals(0, target.status().deliveredEvents());
            assertThrows(IllegalStateException.class, prepared::startTarget);
            publish(settings, 2, "new-");
            int delivered = 0;
            while (delivered < 2 && System.nanoTime() < deadline) {
                var batch = target.poll(Duration.ofMillis(100));
                delivered += batch.records().size();
                if (!batch.isEmpty()) { assertEquals(CommitResult.Status.CONFIRMED,
                        target.commit(batch.processed()).toCompletableFuture().join().status()); }
            }
            assertEquals(2, delivered);
        }
    }

    @Test void preparationCannotBeReusedAfterProcessRestartOrTargetLogRecreation() throws Exception {
        var settings = scenario();
        AtomicBoolean stopped = new AtomicBoolean(true);
        var transition = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), stopped::get);
        var prepared = transition.prepare();
        stopped.set(false);
        assertThrows(IllegalStateException.class, prepared::startTarget);
        stopped.set(true);
        var again = transition.prepare();
        try (Admin admin = Admin.create(KAFKA.bProperties())) {
            admin.deleteTopics(List.of(settings.completionTopic())).all().get(10, TimeUnit.SECONDS);
        }
        assertThrows(IllegalStateException.class, again::startTarget);
    }

    @Test void failedRecoveryProducesNoTargetConsumerOrOffsetReset() throws Exception {
        var settings = scenario();
        // Corrupt input is rejected by data recovery, before any target consumer can start.
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.of(
                "bootstrap.servers", KAFKA.a.getBootstrapServers(), "key.serializer", ByteArraySerializer.class,
                "value.serializer", ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(settings.topic(), 0, new byte[] {1}, new byte[] {2}))
                    .get(10, TimeUnit.SECONDS);
        }
        var transition = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> true);
        assertThrows(RepairException.class, transition::prepare);
        assertFalse(stableMembers(settings, 1));
        try (Admin admin = Admin.create(KAFKA.bProperties())) {
            assertTrue(admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test void conflictingCompletionHistoryPreventsTransitionAfterDataRecovery() throws Exception {
        var settings = scenario();
        publish(settings, 1, "conflict-");
        try (var source = new ActiveKafkaConsumer(settings, KAFKA.aProperties(), KAFKA.bProperties())) {
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            boolean committed = false;
            while (!committed && System.nanoTime() < deadline) {
                var batch = source.poll(Duration.ofMillis(100));
                if (!batch.isEmpty()) {
                    assertEquals(CommitResult.Status.CONFIRMED, source.commit(batch.processed()).toCompletableFuture().join().status());
                    committed = true;
                }
            }
            assertTrue(committed);
        }
        try (var ledger = new CompletionLedger(settings, KAFKA.bProperties())) {
            var marker = ledger.replay(ledger.snapshot()).markers().iterator().next();
            byte[] wrong = marker.hash();
            wrong[0]++;
            ledger.append(new CompletionMarker(marker.id(), marker.partition(), wrong));
        }
        var transition = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> true);
        assertThrows(IllegalStateException.class, transition::prepare);
        try (Admin admin = Admin.create(KAFKA.bProperties())) {
            assertTrue(admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test void runnableDemoVerifiesBothLogsAndWritesDiagnosticEvidence() throws Exception {
        DemoMain.main(new String[] {
            "--bootstrap-a", KAFKA.a.getBootstrapServers(), "--bootstrap-b", KAFKA.b.getBootstrapServers(),
            "--count", "6", "--seed", "11"
        });
        var lines = Files.readAllLines(Path.of("build", "reports", "demo", "events.jsonl"));
        assertEquals(6, lines.stream().filter(line -> line.contains("\"type\":\"publish\"")).count());
        assertEquals(2, lines.stream().filter(line -> line.contains("\"type\":\"audit\"") && line.contains("\"missing\":0")).count());
        assertTrue(lines.stream().anyMatch(line -> line.contains("\"type\":\"success\"") && line.contains("\"completed_redeliveries\":0")));
    }

    @Test void transitionFencesOrphanTransactionsBeforeRestoringCompletionHistory() throws Exception {
        var settings = scenario();
        publish(settings, 1, "pending-");
        try (var ledger = new CompletionLedger(settings, KAFKA.aProperties())) {
            var orphan = ledger.transactionProducer(0);
            try {
                orphan.beginTransaction();
                orphan.send(new CompletionMarker("pending-0", 0, new byte[32]).record(settings.completionTopic()))
                        .get(10, TimeUnit.SECONDS);
                var prepared = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> true).prepare();
                assertThrows(ProducerFencedException.class, orphan::commitTransaction);
                assertEquals(0, prepared.completionReport().completedIds());
                try (var target = prepared.startTarget()) {
                    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                    boolean delivered = false;
                    while (!delivered && System.nanoTime() < deadline) {
                        var batch = target.poll(Duration.ofMillis(100));
                        if (!batch.isEmpty()) {
                            assertEquals("pending-0", batch.records().getFirst().event().eventId());
                            assertEquals(CommitResult.Status.CONFIRMED, target.commit(batch.processed()).toCompletableFuture().join().status());
                            delivered = true;
                        }
                    }
                    assertTrue(delivered);
                }
            } finally { orphan.close(Duration.ZERO); }
        }
    }

    @Test void retainedBeginningChangeAfterPreparationIsRejectedBeforeTargetOffsetReset() throws Exception {
        var settings = scenario();
        publish(settings, 3, "retained-");
        var prepared = new GroupTransition(settings, KAFKA.aProperties(), KAFKA.bProperties(), () -> true).prepare();
        try (Admin admin = Admin.create(KAFKA.bProperties())) {
            admin.deleteRecords(Map.of(new TopicPartition(settings.topic(), 0),
                    RecordsToDelete.beforeOffset(1))).all().get(10, TimeUnit.SECONDS);
        }
        assertThrows(IllegalStateException.class, prepared::startTarget);
        try (Admin admin = Admin.create(KAFKA.bProperties())) {
            assertTrue(admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test void expiredMemberCannotCommitEvenBeforeAnyReplacementOwnerStarts() throws Exception {
        var settings = scenario();
        var calls = new ConcurrentHashMap<String, AtomicInteger>();
        var properties = new HashMap<>(KAFKA.aProperties());
        properties.put("max.poll.interval.ms", 10_000);
        try (var old = new ManagedConsumer(settings, properties, KAFKA.bProperties(), calls, true)) {
            publish(settings, 1, "expired-");
            old.awaitDelivery();
            await(() -> {
                try (Admin admin = Admin.create(KAFKA.aProperties())) {
                    return admin.describeConsumerGroups(List.of(settings.groupId())).all().get(5, TimeUnit.SECONDS)
                            .get(settings.groupId()).members().isEmpty();
                } catch (Exception notReady) { return false; }
            });
            assertFalse(old.stopped()); // Missing group membership is not process termination.
            old.release();
            await(() -> old.lastCommit() != null);
            assertEquals(CommitResult.Status.UNKNOWN, old.lastCommit().status());
        }
        try (var ledger = new CompletionLedger(settings, KAFKA.aProperties())) {
            assertEquals(0, ledger.replay(ledger.snapshot()).size());
        }
    }

    private static ManagedConsumer runner(ConsumerSettings settings, ConcurrentHashMap<String, AtomicInteger> calls) {
        return new ManagedConsumer(settings, KAFKA.aProperties(), KAFKA.bProperties(), calls, false);
    }
    private static ConsumerSettings scenario() throws Exception {
        String topic = "group-" + UUID.randomUUID();
        KAFKA.createTopic(topic);
        return new ConsumerSettings(topic, "group-" + UUID.randomUUID(), Duration.ofSeconds(5), 100, 1_000, 1_000_000, 1_024);
    }
    private static void publish(ConsumerSettings settings, int count, String prefix) throws Exception {
        try (var replication = ReplicationFixture.start(settings.topic(), KAFKA.aProperties(), KAFKA.bProperties());
             var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(PublishConfig.defaults(settings.topic()), KAFKA.aProperties(), KAFKA.bProperties()))) {
            for (int i = 0; i < count; i++) {
                assertTrue(publisher.send(new Event(prefix + i, i % 2 == 0 ? "kafka" : "hello", new byte[] {(byte) i}))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS).successful());
            }
        }
    }
    private static boolean stableMembers(ConsumerSettings settings, int expected) {
        try (Admin admin = Admin.create(KAFKA.aProperties())) {
            var group = admin.describeConsumerGroups(List.of(settings.groupId())).all().get(5, TimeUnit.SECONDS).get(settings.groupId());
            return group.groupState() == GroupState.STABLE && group.members().size() == expected;
        } catch (Exception notReady) { return false; }
    }
    private static void await(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) { LockSupport.parkNanos(Duration.ofMillis(50).toNanos()); }
        assertTrue(condition.getAsBoolean(), "group condition not reached before deadline");
    }
}

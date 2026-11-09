package io.github.keemgdeok.kafkaactiveactive.repair;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;

class RepairServiceTest {
    private final AtomicLong clock = new AtomicLong();
    private final FakeLog a = new FakeLog("a");
    private final FakeLog b = new FakeLog("b");

    private RepairService service(int maxEntries, long maxBytes) {
        return new RepairService(new RepairConfig("events", Duration.ofSeconds(30), Duration.ofSeconds(1),
                maxEntries, maxBytes, 1_024), deadline -> a, deadline -> b, clock::get);
    }

    @Test
    void repairsTheUnionFromEitherSourceAndRepeatedRunsDoNotGrowLogs() {
        a.add("a-original", "kafka", "a");
        b.add("b-original", "hello", "b");
        a.add("both", "kafka", "c");
        b.add("both", "kafka", "c");
        try (var repair = service(100, 100_000)) {
            var first = repair.runOnce();
            assertEquals(3, first.uniqueEvents());
            assertEquals(1, first.copiedToA());
            assertEquals(1, first.copiedToB());
            assertEquals(2, first.clusterA().partitions().get(0).end());
            assertEquals(1, first.clusterB().partitions().get(0).end());
            assertEquals(3, a.records.size());
            assertEquals(3, b.records.size());
            for (int i = 0; i < 5; i++) {
                var repeated = repair.runOnce();
                assertEquals(0, repeated.copiedToA() + repeated.copiedToB());
                assertNotEquals(first.runId(), repeated.runId());
            }
            assertEquals(3, a.records.size());
            assertEquals(3, b.records.size());
            assertNotNull(repair.status().lastCompleted());
            assertFalse(repair.status().running());
        }
    }

    @Test
    void sameIdDifferentPayloadStopsBeforeAnyCopies() {
        a.add("missing", "kafka", "ok");
        a.add("conflict", "hello", "a");
        b.add("conflict", "hello", "b");
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.CONFLICT, repair::runOnce);
            assertEquals(2, a.records.size());
            assertEquals(1, b.records.size());
            assertEquals(RepairException.Reason.CONFLICT, repair.status().lastFailure().reason());
        }
    }

    @Test
    void sameIdDifferentKeyIsAlsoAConflict() {
        a.add("conflict", "kafka", "same payload");
        b.add("conflict", "hello", "same payload");
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.CONFLICT, repair::runOnce);
            assertEquals(1, a.records.size());
            assertEquals(1, b.records.size());
        }
    }

    @Test
    void identicalDuplicatesAreIndexedOnceAndCopiedOnce() {
        a.add("same", "kafka", "value");
        a.add("same", "kafka", "value");
        try (var repair = service(1, 100_000)) {
            var report = repair.runOnce();
            assertEquals(1, report.uniqueEvents());
            assertEquals(2, report.indexedRecords());
            assertEquals(1, report.copiedToB());
            assertEquals(1, b.records.size());
            assertEquals(0, repair.runOnce().copiedToB());
        }
    }

    @Test
    void entryLimitOrByteLimitStopsBeforeCopying() {
        a.add("one", "kafka", "1");
        a.add("two", "hello", "2");
        try (var repair = service(1, 100_000)) {
            assertReason(RepairException.Reason.INDEX_LIMIT, repair::runOnce);
        }
        try (var repair = service(100, 1)) {
            assertReason(RepairException.Reason.INDEX_LIMIT, repair::runOnce);
        }
        assertTrue(b.records.isEmpty());
    }

    @Test
    void finiteTailRefreshFindsLatePeerAppendAndDoesNotCopyItAgain() {
        a.add("delayed", "kafka", "value");
        a.afterScan = () -> b.add("delayed", "kafka", "value");
        try (var repair = service(100, 100_000)) {
            var report = repair.runOnce();
            assertEquals(0, report.copiedToA() + report.copiedToB());
            assertEquals(1, report.clusterB().partitions().get(0).end());
            assertEquals(1, a.records.size());
            assertEquals(1, b.records.size());
        }
    }

    @Test
    void movingTailDoesNotExtendTheCapturedCopyBoundaryForever() {
        a.add("initial", "kafka", "v");
        a.onCapture = call -> {
            if (call > 3) {
                a.add("tail-" + call, "kafka", "v");
            }
        };
        try (var repair = service(100, 100_000)) {
            var report = repair.runOnce();
            assertEquals(1, report.uniqueEvents());
            assertEquals(1, report.clusterA().partitions().get(0).end());
            assertEquals(1, report.copiedToB());
            assertTrue(a.records.size() > 1);
        }
    }

    @Test
    void regeneratedTargetRebuildsIndexEvenWhenItsOffsetsStartAtZeroAgain() {
        a.add("surviving-copy", "kafka", "value");
        b.add("surviving-copy", "kafka", "value");
        try (var repair = service(100, 100_000)) {
            var before = repair.runOnce();
            b.records.clear();
            b.topicId = "new-topic-id";
            var after = repair.runOnce();
            assertNotEquals(before.clusterB().topicId(), after.clusterB().topicId());
            assertEquals(0, after.clusterB().partitions().get(0).end());
            assertEquals(1, after.copiedToB());
            assertEquals(1, b.records.size());
        }
    }

    @Test
    void topicGenerationChangeDuringIndexingFailsWithoutUsingOldPresenceFlags() {
        a.add("one", "kafka", "value");
        b.onCapture = call -> {
            if (call == 3) {
                b.topicId = "regenerated";
            }
        };
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.LOG_CHANGED, repair::runOnce);
            assertTrue(b.records.isEmpty());
        }
    }

    @Test
    void retentionAdvanceAndLeaderEpochChangeAreRejected() {
        var original = a.capture();
        a.beginning = 1;
        assertReason(RepairException.Reason.LOG_CHANGED, () -> original.requireContinuation(a.capture()));
        a.beginning = 0;
        a.epoch++;
        assertReason(RepairException.Reason.LOG_CHANGED, () -> original.requireContinuation(a.capture()));
    }

    @Test
    void sameClusterUnderDifferentAddressesIsRejected() {
        b.clusterId = a.clusterId;
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.INVALID_TOPOLOGY, repair::runOnce);
        }
    }

    @Test
    void malformedHeadersAndUnsupportedVersionFailExplicitly() {
        var record = a.add("id", "kafka", "value");
        record.headers().add(RoutedKafkaPublisher.EVENT_ID_HEADER, new byte[] {1});
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.INVALID_RECORD, repair::runOnce);
            assertTrue(b.records.isEmpty());
        }
        record.headers().remove(RoutedKafkaPublisher.EVENT_ID_HEADER)
                .add(RoutedKafkaPublisher.EVENT_ID_HEADER, new byte[] {'i', 'd'});
        record.headers().remove(RoutedKafkaPublisher.FORMAT_HEADER)
                .add(RoutedKafkaPublisher.FORMAT_HEADER, new byte[] {2});
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.INVALID_RECORD, repair::runOnce);
        }
    }

    @Test
    void deadlineIsCheckedWithinScansAndDoesNotProduceFalseCompletion() {
        a.add("id", "kafka", "value");
        a.beforeScan = () -> clock.set(Duration.ofSeconds(30).toNanos());
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.DEADLINE, repair::runOnce);
            assertEquals(null, repair.status().lastCompleted());
            assertEquals(RepairException.Reason.DEADLINE, repair.status().lastFailure().reason());
            assertTrue(b.records.isEmpty());
        }
    }

    @Test
    void partialAckThenFailureIsRecoverableWithoutReplayingTheAcknowledgedCopy() {
        a.add("one", "kafka", "v1");
        a.add("two", "kafka", "v2");
        b.failAppendAt = 2;
        try (var repair = service(100, 100_000)) {
            assertReason(RepairException.Reason.CLIENT_FAILURE, repair::runOnce);
            assertEquals(1, repair.status().lastFailure().copiedToB());
            assertEquals(1, b.records.size());
            b.failAppendAt = -1;
            var recovered = repair.runOnce();
            assertEquals(1, recovered.copiedToB());
            assertEquals(2, b.records.size());
            assertEquals(0, repair.runOnce().copiedToB());
        }
    }

    @Test
    void overlappingRunsAreRejectedAndClosingPreventsNewRuns() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        a.beforeScan = () -> {
            entered.countDown();
            try {
                assertTrue(release.await(3, TimeUnit.SECONDS));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new RepairException(RepairException.Reason.CLOSED, "test owner interrupted");
            }
        };
        var repair = service(100, 100_000);
        try (var caller = Executors.newSingleThreadExecutor()) {
            var running = caller.submit(repair::runOnce);
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertReason(RepairException.Reason.BUSY, repair::runOnce);
            release.countDown();
            running.get(2, TimeUnit.SECONDS);
            repair.close();
            assertReason(RepairException.Reason.CLOSED, repair::runOnce);
        } finally {
            release.countDown();
            repair.close();
        }
    }

    private static void assertReason(RepairException.Reason reason, Runnable operation) {
        assertEquals(reason, assertThrows(RepairException.class, operation::run).reason());
    }

    @Test
    void closeInterruptsAndWaitsForThePeriodicOwnerToReleaseItsResources() throws Exception {
        var entered = new CountDownLatch(1);
        var neverReleased = new CountDownLatch(1);
        a.beforeScan = () -> {
            entered.countDown();
            try {
                neverReleased.await();
            } catch (InterruptedException expected) {
                Thread.currentThread().interrupt();
                throw new RepairException(RepairException.Reason.CLOSED, "periodic owner interrupted");
            }
        };
        var repair = service(100, 100_000);
        try {
            repair.start();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            repair.close();
            assertFalse(repair.status().running());
            assertEquals(RepairException.Reason.CLOSED, repair.status().lastFailure().reason());
            assertEquals(1, a.closes);
            assertEquals(1, b.closes);
        } finally {
            neverReleased.countDown();
            repair.close();
        }
    }

    private static final class FakeLog implements RepairLog {
        private final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        private String clusterId;
        private String topicId;
        private int captures;
        private int closes;
        private int appends;
        private long beginning;
        private int epoch = 1;
        private int failAppendAt = -1;
        private Runnable beforeScan = () -> {};
        private Runnable afterScan = () -> {};
        private IntConsumer onCapture = ignored -> {};

        private FakeLog(String name) {
            clusterId = name;
            topicId = name + "-topic";
        }

        private ConsumerRecord<byte[], byte[]> add(String id, String key, String payload) {
            int partition = new Event(id, key, new byte[0]).partition();
            long offset = records.stream().filter(record -> record.partition() == partition).count();
            var record = new ConsumerRecord<>("events", partition, offset,
                    key.getBytes(StandardCharsets.UTF_8), payload.getBytes(StandardCharsets.UTF_8));
            record.headers().add(RoutedKafkaPublisher.EVENT_ID_HEADER, id.getBytes(StandardCharsets.UTF_8));
            record.headers().add(RoutedKafkaPublisher.FORMAT_HEADER, new byte[] {1});
            records.add(record);
            return record;
        }

        @Override
        public RepairReport.LogBoundary capture() {
            onCapture.accept(++captures);
            return new RepairReport.LogBoundary(clusterId, topicId, Map.of(
                    0, new RepairReport.PartitionBoundary(beginning,
                            records.stream().filter(r -> r.partition() == 0).count(), epoch),
                    1, new RepairReport.PartitionBoundary(beginning,
                            records.stream().filter(r -> r.partition() == 1).count(), epoch)));
        }

        @Override
        public void scan(RepairReport.LogBoundary boundary, Map<Integer, Long> from,
                          Consumer<ConsumerRecord<byte[], byte[]>> visitor) {
            Runnable before = beforeScan;
            beforeScan = () -> {};
            before.run();
            for (var record : List.copyOf(records)) {
                if (record.offset() >= from.get(record.partition())
                        && record.offset() < boundary.partitions().get(record.partition()).end()) {
                    visitor.accept(record);
                }
            }
            Runnable after = afterScan;
            afterScan = () -> {};
            after.run();
        }

        @Override
        public void append(ConsumerRecord<byte[], byte[]> record) {
            if (++appends == failAppendAt) {
                throw new IllegalStateException("injected append failure");
            }
            add(new String(record.headers().lastHeader(RoutedKafkaPublisher.EVENT_ID_HEADER).value(),
                    StandardCharsets.UTF_8), new String(record.key(), StandardCharsets.UTF_8),
                    new String(record.value(), StandardCharsets.UTF_8));
        }

        @Override
        public void close() { closes++; }
    }
}

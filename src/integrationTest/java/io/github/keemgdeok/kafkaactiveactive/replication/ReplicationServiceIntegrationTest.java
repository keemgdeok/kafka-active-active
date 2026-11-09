package io.github.keemgdeok.kafkaactiveactive.replication;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairService;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.NativeLog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ReplicationServiceIntegrationTest {
    private static final KafkaPair PAIR = new KafkaPair("streaming-replication");
    @BeforeAll static void start() { PAIR.start(); }
    @AfterAll static void stop() throws Exception { PAIR.close(); }

    @Test void rawCopiesKeepPartitionAndAckEvidenceWhileCommittedRestartDoesNotGrowLogs() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        var x = new Event("a-original", "kafka", new byte[] {0, -1, 1});
        var y = new Event("b-original", "hello", new byte[] {2});
        var attemptX = UUID.randomUUID();
        var attemptY = UUID.randomUUID();
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service);
            write(PAIR.aProperties(), EventCodec.raw(topic, x, attemptX));
            write(PAIR.bProperties(), EventCodec.raw(topic, y, attemptY));
            await(() -> service.status().aToB().replicatedRecords() == 1
                    && service.status().bToA().replicatedRecords() == 1, service);
            for (var properties : List.of(PAIR.aProperties(), PAIR.bProperties())) {
                var records = read(properties, topic);
                assertEquals(2, records.size());
                assertEquals(Set.of(x, y), records.stream().map(r -> EventCodec.event(r, 1_024))
                        .collect(java.util.stream.Collectors.toSet()));
                assertEquals(1, records.stream().filter(r -> EventCodec.kind(r) == EventCodec.Kind.RAW).count());
                assertEquals(1, records.stream().filter(r -> EventCodec.kind(r) == EventCodec.Kind.REPLICA).count());
            }
            await(() -> service.status().aToB().committedOffsets().equals(Map.of(0, 1L, 1, 1L))
                    && service.status().bToA().committedOffsets().equals(Map.of(0, 1L, 1, 1L)), service);
            var ackA = ReplicationAck.decode(NativeLog.awaitVisibleCount(PAIR.aProperties(), config.confirmationTopic(),1).getFirst(), 1_024);
            var ackB = ReplicationAck.decode(NativeLog.awaitVisibleCount(PAIR.bProperties(), config.confirmationTopic(),1).getFirst(), 1_024);
            assertEquals(attemptX, ackA.attempt()); assertEquals(attemptY, ackB.attempt());
            assertEquals(service.status().aToB().source(), ackA.source());
            assertEquals(service.status().aToB().target(), ackA.target());
            assertEquals(0, ackA.partition()); assertEquals(1, ackB.partition());
            assertArrayEquals(EventCodec.hash(x.key(), x.payload()), ackA.hash());
        }
        try (var restarted = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            restarted.start(); ready(restarted);
            assertEquals(Map.of(0, 1L, 1, 1L), restarted.status().aToB().committedOffsets());
            assertEquals(Map.of(0, 1L, 1, 1L), restarted.status().bToA().committedOffsets());
            assertEquals(2, read(PAIR.aProperties(), topic).size());
            assertEquals(2, read(PAIR.bProperties(), topic).size());
        }
    }

    @Test void ongoingSameGenerationPrefixDeletionFailsBothDirectionsInsteadOfSilentlyContinuing() throws Exception {
        String topic=topic();
        write(PAIR.aProperties(),EventCodec.raw(topic,new Event("delete-prefix","kafka",new byte[]{1}),UUID.randomUUID()));
        try(var service=new ReplicationService(serviceConfig(topic),PAIR.aProperties(),PAIR.bProperties())) {
            service.start(); ready(service);
            await(() -> service.status().aToB().committedOffsets().getOrDefault(0,0L)==1,service);
            var before=service.status().aToB().source();
            try(var admin=Admin.create(PAIR.aProperties())) {
                admin.deleteRecords(Map.of(new TopicPartition(topic,0),org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(1)))
                        .all().get(5,TimeUnit.SECONDS);
            }
            long end=System.nanoTime()+Duration.ofSeconds(10).toNanos();
            while(System.nanoTime()<end && (service.status().aToB().state()!=ReplicationService.State.FAILED
                    || service.status().bToA().state()!=ReplicationService.State.FAILED)) {
                LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            }
            assertEquals(ReplicationService.State.FAILED,service.status().aToB().state());
            assertEquals(ReplicationService.State.FAILED,service.status().bToA().state());
            assertEquals(before,service.status().aToB().source(),"same UUID deletion is not a new log generation");
        }
    }

    @Test void rawsInFailoverPartitionsAreAlsoForwarded() throws Exception {
        String topic = topic();
        try (var service = new ReplicationService(ReplicationConfig.defaults(topic),
                PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service);
            write(PAIR.aProperties(), EventCodec.raw(topic, new Event("fallback-to-a", "hello", new byte[] {1}), UUID.randomUUID()));
            write(PAIR.bProperties(), EventCodec.raw(topic, new Event("fallback-to-b", "kafka", new byte[] {2}), UUID.randomUUID()));
            await(() -> service.status().aToB().replicatedRecords() == 1
                    && service.status().bToA().replicatedRecords() == 1, service);
            assertEquals(2, read(PAIR.aProperties(), topic).size());
            assertEquals(2, read(PAIR.bProperties(), topic).size());
        }
    }

    @Test void legacyAndReplicaAreSkippedButFiniteRepairCanUseBothWithoutRecirculation() throws Exception {
        String topic = topic();
        var legacy = new Event("legacy", "kafka", new byte[] {1});
        var other = new Event("replica-source", "hello", new byte[] {2});
        write(PAIR.aProperties(), EventCodec.encode(topic, legacy));
        write(PAIR.bProperties(), EventCodec.replica(topic,
                consumed(EventCodec.raw(topic, other, UUID.randomUUID())), 1_024));
        try (var service = new ReplicationService(ReplicationConfig.defaults(topic),
                PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service);
            await(() -> service.status().aToB().committedOffsets().getOrDefault(0, 0L) == 1
                    && service.status().bToA().committedOffsets().getOrDefault(1, 0L) == 1, service);
            assertEquals(1, read(PAIR.aProperties(), topic).size());
            assertEquals(1, read(PAIR.bProperties(), topic).size());
            try (var repair = new RepairService(RepairConfig.defaults(topic), PAIR.aProperties(), PAIR.bProperties())) {
                assertEquals(2, repair.runOnce().uniqueEvents());
                assertEquals(0, repair.runOnce().copiedToA());
            }
            await(() -> service.status().aToB().committedOffsets().equals(Map.of(0, 1L, 1, 1L))
                    && service.status().bToA().committedOffsets().equals(Map.of(0, 1L, 1, 1L)), service);
            assertEquals(0, service.status().aToB().replicatedRecords());
            assertEquals(0, service.status().bToA().replicatedRecords());
            assertEquals(2, read(PAIR.aProperties(), topic).size());
            assertEquals(2, read(PAIR.bProperties(), topic).size());
            assertTrue(read(PAIR.aProperties(), serviceConfig(topic).confirmationTopic()).isEmpty());
        }
    }

    @Test void targetAckPrecedesAtomicReceiptAndSourceOffsetCommit() throws Exception {
        String topic = topic();
        var config = serviceConfig(topic);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var once = new AtomicBoolean();
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties(), point -> {
            if (point.sourceSide() == 0 && point.stage() == ReplicationService.Stage.BEFORE_SOURCE_COMMIT
                    && once.compareAndSet(false, true)) {
                entered.countDown();
                try {
                    if (!release.await(20, TimeUnit.SECONDS)) { throw new AssertionError("commit gate not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
            }
        })) {
            service.start(); ready(service);
            write(PAIR.aProperties(), EventCodec.raw(topic, new Event("atomic", "kafka", new byte[] {1}), UUID.randomUUID()));
            assertTrue(entered.await(15, TimeUnit.SECONDS), service.status().toString());
            try {
                assertEquals(1, read(PAIR.bProperties(), topic).size(), "target data already ACKed");
                assertTrue(read(PAIR.aProperties(), config.confirmationTopic()).isEmpty(), "uncommitted receipt is invisible");
                var state = service.status().aToB();
                try (var admin = Admin.create(PAIR.aProperties())) {
                    var committed = admin.listConsumerGroupOffsets(config.groupId(state.source(), state.target()))
                            .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
                    assertTrue(committed == null || committed.offset() == 0);
                }
            } finally { release.countDown(); }
            await(() -> service.status().aToB().committedOffsets().getOrDefault(0, 0L) == 1, service);
            assertEquals(1, NativeLog.awaitVisibleCount(PAIR.aProperties(), config.confirmationTopic(),1).size());
        } finally { release.countDown(); }
    }

    @Test void recreatedTargetUsesNewGenerationAndReplaysRawFromZero() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service);
            write(PAIR.aProperties(), EventCodec.raw(topic, new Event("before-reset", "kafka", new byte[] {1}), UUID.randomUUID()));
            await(() -> service.status().aToB().replicatedRecords() == 1, service);
            var old = service.status().aToB().target();
            try (var admin = Admin.create(PAIR.bProperties())) {
                admin.deleteTopics(List.of(topic)).all().get(5, TimeUnit.SECONDS);
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                        "cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1"))))
                        .all().get(5, TimeUnit.SECONDS);
            }
            await(() -> service.status().aToB().state() == ReplicationService.State.RUNNING
                    && service.status().aToB().target() != null
                    && !old.equals(service.status().aToB().target())
                    && service.status().aToB().replicatedRecords() >= 2, service);
            assertEquals("before-reset", EventCodec.event(read(PAIR.bProperties(), topic).getFirst(), 1_024).eventId());
        }
    }

    @Test void unavailableTargetCannotAdvanceSourceAndRecoveryCopiesThePendingRaw() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        var beforeAppend = new CountDownLatch(1);
        var allowAppend = new CountDownLatch(1);
        var once = new AtomicBoolean();
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties(), point -> {
            if (point.sourceSide() == 0 && point.stage() == ReplicationService.Stage.BEFORE_TARGET_APPEND
                    && once.compareAndSet(false, true)) {
                beforeAppend.countDown();
                try {
                    if (!allowAppend.await(15, TimeUnit.SECONDS)) { throw new AssertionError("append gate not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
            }
        })) {
            service.start(); ready(service);
            var saved = service.status().aToB();
            write(PAIR.aProperties(), EventCodec.raw(topic,
                    new Event("blocked-target", "kafka", new byte[] {1}), UUID.randomUUID()));
            assertTrue(beforeAppend.await(15, TimeUnit.SECONDS), service.status().toString());
            PAIR.b.getDockerClient().pauseContainerCmd(PAIR.b.getContainerId()).exec();
            try {
                allowAppend.countDown();
                await(() -> service.status().aToB().state() == ReplicationService.State.RETRYING, service);
                try (var admin = Admin.create(PAIR.aProperties())) {
                    var offset = admin.listConsumerGroupOffsets(config.groupId(saved.source(), saved.target()))
                            .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
                    assertTrue(offset == null || offset.offset() == 0, "unreplicated input was skipped");
                }
                assertTrue(read(PAIR.aProperties(), config.confirmationTopic()).isEmpty());
            } finally { PAIR.b.getDockerClient().unpauseContainerCmd(PAIR.b.getContainerId()).exec(); }
            await(() -> service.status().aToB().replicatedRecords() >= 1, service);
            assertTrue(read(PAIR.bProperties(), topic).stream().anyMatch(r ->
                    EventCodec.event(r, 1_024).eventId().equals("blocked-target")));
        }
        finally { allowAppend.countDown(); }
    }

    @Test void sameGenerationOutOfRangeProgressFailsInsteadOfSkipping() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        ReplicationService.DirectionStatus saved;
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service); saved = service.status().aToB();
        }
        try (var admin = Admin.create(PAIR.aProperties())) {
            admin.alterConsumerGroupOffsets(config.groupId(saved.source(), saved.target()),
                    Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(123)))
                    .all().get(10, TimeUnit.SECONDS);
        }
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            service.start();
            await(() -> service.status().aToB().state() == ReplicationService.State.FAILED, service);
            assertTrue(service.status().aToB().failure().contains("outside retained log"));
            assertTrue(read(PAIR.bProperties(), topic).isEmpty());
        }
    }

    @Test void nonzeroProgressWithoutLeaderEpochFailsClosed() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        ReplicationService.DirectionStatus saved;
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service); saved = service.status().aToB();
        }
        write(PAIR.aProperties(), EventCodec.raw(topic,
                new Event("unreplicated", "kafka", new byte[] {1}), UUID.randomUUID()));
        try (var admin = Admin.create(PAIR.aProperties())) {
            admin.alterConsumerGroupOffsets(config.groupId(saved.source(), saved.target()),
                    Map.of(new TopicPartition(topic, 0), new OffsetAndMetadata(1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
            service.start();
            await(() -> service.status().aToB().state() == ReplicationService.State.FAILED, service);
            assertTrue(service.status().aToB().failure().contains("missing leader epoch"));
            assertTrue(read(PAIR.bProperties(), topic).isEmpty());
        }
    }

    @Test void slowBatchCommitsAckedPrefixAndRewindsOnlyUnprocessedRecords() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        var clock = new java.util.concurrent.atomic.AtomicLong();
        var firstAck = new AtomicBoolean();
        var committedPrefix = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var firstCommit = new AtomicBoolean();
        var firstBatchSize = new java.util.concurrent.atomic.AtomicInteger();
        for (int i = 0; i < 3; i++) {
            write(PAIR.aProperties(), EventCodec.raw(topic,
                    new Event("slow-" + i, "kafka", new byte[] {(byte) i}), UUID.randomUUID()));
        }
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties(), point -> {
            if (point.sourceSide() != 0) { return; }
            if (point.stage() == ReplicationService.Stage.BATCH_READ) {
                firstBatchSize.compareAndSet(0, point.batchSize());
            }
            if (point.stage() == ReplicationService.Stage.AFTER_TARGET_ACK && firstAck.compareAndSet(false, true)) {
                clock.addAndGet(Duration.ofSeconds(6).toNanos());
            }
            if (point.stage() == ReplicationService.Stage.AFTER_SOURCE_COMMIT && firstCommit.compareAndSet(false, true)) {
                committedPrefix.countDown();
                try {
                    if (!release.await(15, TimeUnit.SECONDS)) { throw new AssertionError("prefix gate not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
            }
        }, clock::get)) {
            service.start();
            assertTrue(committedPrefix.await(15, TimeUnit.SECONDS), service.status().toString());
            try {
                assertEquals(3, firstBatchSize.get(), "fixture must exercise an actual partial batch");
                assertEquals(1, read(PAIR.bProperties(), topic).size());
                assertEquals(1, NativeLog.awaitVisibleCount(PAIR.aProperties(), config.confirmationTopic(),1).size());
            } finally { release.countDown(); }
            await(() -> service.status().aToB().committedOffsets().getOrDefault(0, 0L) == 3, service);
            assertEquals(3, read(PAIR.bProperties(), topic).size(), "committed prefix must not be copied again");
        } finally { release.countDown(); }
    }

    @Test void byteLimitedBatchCommitsOnePartitionWithoutSkippingTheOther() throws Exception {
        String topic = topic();
        var config = ReplicationConfig.defaults(topic);
        var gate = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var once = new AtomicBoolean();
        var firstBatchSize = new java.util.concurrent.atomic.AtomicInteger();
        write(PAIR.aProperties(), EventCodec.raw(topic, new Event("large-p0", "kafka", new byte[700_000]), UUID.randomUUID()));
        write(PAIR.aProperties(), EventCodec.raw(topic, new Event("large-p1", "hello", new byte[700_000]), UUID.randomUUID()));
        try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties(), point -> {
            if (point.sourceSide() != 0) { return; }
            if (point.stage() == ReplicationService.Stage.BATCH_READ) { firstBatchSize.compareAndSet(0, point.batchSize()); }
            if (point.stage() == ReplicationService.Stage.AFTER_SOURCE_COMMIT && once.compareAndSet(false, true)) {
                gate.countDown();
                try {
                    if (!release.await(15, TimeUnit.SECONDS)) { throw new AssertionError("byte gate not released"); }
                } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
            }
        })) {
            service.start();
            assertTrue(gate.await(15, TimeUnit.SECONDS), service.status().toString());
            try {
                assertEquals(2, firstBatchSize.get(), "fixture must return both partitions in one batch");
                assertEquals(1, read(PAIR.bProperties(), topic).size());
            } finally { release.countDown(); }
            await(() -> service.status().aToB().committedOffsets().equals(Map.of(0, 1L, 1, 1L)), service);
            assertEquals(2, read(PAIR.bProperties(), topic).size());
        } finally { release.countDown(); }
    }

    @Test void malformedUtf8RawStopsDirectionWithoutAdvancingOrForwarding() throws Exception {
        String topic = topic();
        try (var service = new ReplicationService(ReplicationConfig.defaults(topic), PAIR.aProperties(), PAIR.bProperties())) {
            service.start(); ready(service);
            var record = EventCodec.raw(topic, new Event("bad-frame", "kafka", new byte[] {1}), UUID.randomUUID());
            record.headers().remove(EventCodec.EVENT_ID_HEADER).add(EventCodec.EVENT_ID_HEADER, new byte[] {(byte) 255});
            write(PAIR.aProperties(), record);
            await(() -> service.status().aToB().state() == ReplicationService.State.FAILED, service);
            assertTrue(service.status().aToB().failure().startsWith("IllegalArgumentException:"));
            assertTrue(service.status().aToB().failure().contains("must be valid UTF-8"));
            assertEquals(0L, service.status().aToB().committedOffsets().getOrDefault(0, 0L));
            assertTrue(read(PAIR.bProperties(), topic).isEmpty());
        }
    }

    private static ReplicationConfig serviceConfig(String topic) {
        return new ReplicationConfig(topic, Duration.ofSeconds(10), 100, 1_024, Duration.ofSeconds(5));
    }
    private static String topic() throws Exception {
        String topic = "replication-" + UUID.randomUUID();
        PAIR.createTopic(topic); return topic;
    }
    private static void write(Map<String, Object> properties, ProducerRecord<byte[], byte[]> record) throws Exception {
        try (var producer = new KafkaProducer<byte[], byte[]>(KafkaAccess.writer(properties, Duration.ofSeconds(5), 900_000))) {
            producer.send(record).get(10, TimeUnit.SECONDS);
        }
    }
    static void ready(ReplicationService service) {
        await(() -> service.status().aToB().state() == ReplicationService.State.RUNNING
                && service.status().bToA().state() == ReplicationService.State.RUNNING
                && service.status().aToB().committedOffsets().size() == 2
                && service.status().bToA().committedOffsets().size() == 2, service);
    }
    private static void await(BooleanSupplier condition, ReplicationService service) {
        long end = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < end) { LockSupport.parkNanos(20_000_000); }
        assertTrue(condition.getAsBoolean(), service.status().toString());
    }
    private static List<ConsumerRecord<byte[], byte[]>> read(Map<String, Object> properties, String topic) {
        var consumer = new KafkaConsumer<byte[], byte[]>(KafkaAccess.reader(properties, Duration.ofSeconds(5), 100, 900_000));
        try {
            var partitions = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));
            consumer.assign(partitions);
            var ends = consumer.endOffsets(partitions);
            consumer.seekToBeginning(partitions);
            var result = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            long end = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                if (System.nanoTime() >= end) { throw new AssertionError("finite read timed out"); }
                consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                    if (record.offset() < ends.get(new TopicPartition(topic, record.partition()))) { result.add(record); }
                });
            }
            return result;
        } finally { consumer.close(CloseOptions.timeout(Duration.ofSeconds(1))); }
    }
    private static ConsumerRecord<byte[], byte[]> consumed(ProducerRecord<byte[], byte[]> record) {
        return new ConsumerRecord<>(record.topic(), record.partition(), 0, 0, TimestampType.CREATE_TIME,
                record.key().length, record.value().length, record.key(), record.value(), record.headers(), Optional.empty());
    }
}

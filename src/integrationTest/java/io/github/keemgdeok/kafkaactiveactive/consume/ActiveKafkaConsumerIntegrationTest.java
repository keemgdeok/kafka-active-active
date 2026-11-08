package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.github.keemgdeok.kafkaactiveactive.publish.DualKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ActiveKafkaConsumerIntegrationTest {
    private static final KafkaPair KAFKA = new KafkaPair("consumer-brokers");
    @BeforeAll static void start() { KAFKA.start(); }
    @AfterAll static void stop() throws Exception { KAFKA.close(); }

    @Test void completedIdBlocksHandlerAfterColdRestartEvenWhenInputOffsetsAreRewound() throws Exception {
        var settings = scenario();
        var event = event("one", (byte) 1);
        publish(settings, List.of(event, event));
        try (var consumer = consumer(settings)) {
            var batch = awaitBatch(consumer);
            assertEquals(1, batch.records().size());
            var result = consumer.commit(batch.processed()).toCompletableFuture().join();
            assertEquals(CommitResult.Status.CONFIRMED, result.status());
            assertEquals(Set.of("one"), result.locallyCompletedIds());
            assertEquals(Set.of("one"), result.peerConfirmedIds());
        }
        reset(settings, KAFKA.aProperties());
        try (var consumer = consumer(settings)) {
            awaitFiltered(consumer, 2);
            assertEquals(0, consumer.status().deliveredEvents());
        }
        try (var ledger = new CompletionLedger(settings, KAFKA.aProperties())) {
            assertEquals(1, ledger.replay(ledger.snapshot()).size());
        }
    }

    @Test void unfinishedEventIsDeliveredAgainAfterConsumerClosesWithoutCompletion() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("unfinished", (byte) 1)));
        try (var consumer = consumer(settings)) { assertEquals("unfinished", awaitBatch(consumer).records().getFirst().event().eventId()); }
        try (var consumer = consumer(settings)) {
            var batch = awaitBatch(consumer);
            assertEquals("unfinished", batch.records().getFirst().event().eventId());
            assertEquals(CommitResult.Status.CONFIRMED, consumer.commit(batch.processed()).toCompletableFuture().join().status());
        }
    }

    @Test void partialCommitRewindsUnfinishedRecordsAndDoesNotSkipTheirOffsets() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1), event("two", (byte) 2), event("three", (byte) 3)));
        try (var consumer = consumer(settings)) {
            var batch = awaitBatch(consumer);
            assertEquals(3, batch.records().size());
            assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
            assertEquals(CommitResult.Status.CONFIRMED,
                    consumer.commit(batch.processedPrefix(1)).toCompletableFuture().join().status());
            assertEquals(1, offset(settings));
            var remaining = awaitBatch(consumer);
            assertEquals(List.of("two", "three"), remaining.records().stream().map(d -> d.event().eventId()).toList());
            assertThrows(IllegalArgumentException.class, () -> consumer.commit(batch.processed()));
            consumer.commit(remaining.processed()).toCompletableFuture().join();
            assertEquals(3, offset(settings));
        }
    }

    @Test void failureBeforeTransactionCommitLeavesNeitherCompletionNorCommittedInputOffset() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1)));
        try (var consumer = new ActiveKafkaConsumer(settings, KAFKA.aProperties(), KAFKA.bProperties(), (partition, point) -> {
            if (partition == 0 && point == ActiveKafkaConsumer.CommitPoint.OFFSETS_STAGED) {
                throw new IllegalStateException("fault before local commit");
            }
        })) {
            var result = consumer.commit(awaitBatch(consumer).processed()).toCompletableFuture().join();
            assertEquals(CommitResult.Status.UNKNOWN, result.status());
            assertThrows(IllegalStateException.class, () -> consumer.poll(Duration.ZERO));
        }
        assertEquals(-1, offset(settings));
        try (var consumer = consumer(settings)) {
            var batch = awaitBatch(consumer); // initTransactions aborts the old in-flight transaction.
            try (var ledger = new CompletionLedger(settings, KAFKA.aProperties())) {
                assertEquals(0, ledger.replay(ledger.snapshot()).size());
            }
            assertEquals("one", batch.records().getFirst().event().eventId());
            consumer.commit(batch.processed()).toCompletableFuture().join();
        }
    }

    @Test void lostResponseAfterLocalCommitStillBlocksRedeliveryAndHistoryCanBeRepaired() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1)));
        try (var consumer = new ActiveKafkaConsumer(settings, KAFKA.aProperties(), KAFKA.bProperties(), (partition, point) -> {
            if (partition == 0 && point == ActiveKafkaConsumer.CommitPoint.LOCAL_COMMITTED) {
                throw new IllegalStateException("fault after local commit before response/cache/peer");
            }
        })) {
            assertEquals(CommitResult.Status.UNKNOWN,
                    consumer.commit(awaitBatch(consumer).processed()).toCompletableFuture().join().status());
        }
        assertEquals(1, offset(settings));
        var repaired = new CompletionReplicator(settings, KAFKA.aProperties(), KAFKA.bProperties()).runOnce();
        assertEquals(1, repaired.copiedToB());
        reset(settings, KAFKA.aProperties());
        try (var consumer = consumer(settings)) { awaitFiltered(consumer, 1); }
    }

    @Test void previousPartitionOwnerIsFencedByTheStableTransactionalIdentity() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1)));
        try (var old = consumer(settings)) {
            var batch = awaitBatch(old);
            try (var ledger = new CompletionLedger(settings, KAFKA.aProperties());
                 var replacement = ledger.transactionProducer(0)) {
                assertNotNull(replacement);
                assertEquals(CommitResult.Status.UNKNOWN, old.commit(batch.processed()).toCompletableFuture().join().status());
            }
        }
        try (var current = consumer(settings)) {
            var batch = awaitBatch(current);
            assertEquals("one", batch.records().getFirst().event().eventId());
            current.commit(batch.processed()).toCompletableFuture().join();
        }
    }

    @Test void peerOutageIsLocalSuccessAndReconciliationRequiresStoppedGroup() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1)));
        KAFKA.b.getDockerClient().pauseContainerCmd(KAFKA.b.getContainerId()).exec();
        try {
            try (var consumer = consumer(settings)) {
                var result = consumer.commit(awaitBatch(consumer).processed()).toCompletableFuture().join();
                assertEquals(CommitResult.Status.LOCAL_ONLY, result.status());
                assertEquals(Set.of("one"), result.locallyCompletedIds());
            }
        } finally { KAFKA.b.getDockerClient().unpauseContainerCmd(KAFKA.b.getContainerId()).exec(); }
        var replicator = new CompletionReplicator(settings, KAFKA.aProperties(), KAFKA.bProperties());
        assertEquals(1, replicator.runOnce().copiedToB());
        assertEquals(0, replicator.runOnce().copiedToB());
        try (var consumer = new ActiveKafkaConsumer(settings, KAFKA.bProperties(), KAFKA.aProperties())) {
            awaitFiltered(consumer, 1);
            assertThrows(IllegalStateException.class, replicator::runOnce);
        }
    }

    @Test void missingCompletionHistoryIsNotSilentlyReplacedWhenNativeOffsetsExist() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1)));
        try (var consumer = consumer(settings)) { consumer.commit(awaitBatch(consumer).processed()).toCompletableFuture().join(); }
        try (Admin admin = Admin.create(KAFKA.aProperties())) {
            admin.deleteTopics(List.of(settings.completionTopic())).all().get(10, TimeUnit.SECONDS);
        }
        try (var consumer = consumer(settings)) {
            Throwable failure = assertThrows(RuntimeException.class, () -> awaitBatch(consumer));
            while (failure.getCause() != null) { failure = failure.getCause(); }
            assertInstanceOf(IllegalStateException.class, failure);
            assertTrue(failure.getMessage().contains("missing/different completion history"));
            assertEquals(0, consumer.status().deliveredEvents());
        }
    }

    @Test void completedIdWithChangedPayloadFailsBeforeHandlerAndDifferentGroupsRemainIndependent() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("one", (byte) 1)));
        try (var consumer = consumer(settings)) { consumer.commit(awaitBatch(consumer).processed()).toCompletableFuture().join(); }
        var other = new ConsumerSettings(settings.topic(), "other-" + UUID.randomUUID(), settings.operationTimeout(),
                100, 1_000, 1_000_000, 1_024);
        try (var consumer = new ActiveKafkaConsumer(other, KAFKA.bProperties(), KAFKA.aProperties())) {
            assertEquals("one", awaitBatch(consumer).records().getFirst().event().eventId());
        }
        publish(settings, List.of(event("one", (byte) 2)));
        try (var consumer = consumer(settings)) {
            assertThrows(IllegalStateException.class, () -> awaitBatch(consumer));
            assertEquals(0, consumer.status().deliveredEvents());
        }
    }

    @Test void restoredOldOffsetsCannotBeAppliedToARecreatedInputTopic() throws Exception {
        var settings = scenario();
        publish(settings, List.of(event("old", (byte) 1)));
        try (var consumer = consumer(settings)) { consumer.commit(awaitBatch(consumer).processed()).toCompletableFuture().join(); }
        OffsetAndMetadata oldOffset;
        try (Admin admin = Admin.create(KAFKA.aProperties())) {
            oldOffset = admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(settings.topic(), 0));
            assertNotNull(oldOffset);
            admin.deleteTopics(List.of(settings.topic())).all().get(10, TimeUnit.SECONDS);
            admin.createTopics(List.of(new org.apache.kafka.clients.admin.NewTopic(settings.topic(), 2, (short) 1)
                    .configs(Map.of("retention.ms", "-1", "retention.bytes", "-1"))))
                    .all().get(10, TimeUnit.SECONDS);
        }
        publish(settings, List.of(event("new-0", (byte) 2), event("new-1", (byte) 3)));
        // Topic deletion can clear Kafka's group offsets. Explicitly restore the stale checkpoint
        // to reproduce the unsafe offset reuse that the SDK must reject.
        try (Admin admin = Admin.create(KAFKA.aProperties())) {
            admin.alterConsumerGroupOffsets(settings.groupId(), Map.of(new TopicPartition(settings.topic(), 0), oldOffset))
                    .all().get(10, TimeUnit.SECONDS);
        }
        try (var consumer = consumer(settings)) {
            Throwable failure = assertThrows(RuntimeException.class, () -> awaitBatch(consumer));
            while (failure.getCause() != null) { failure = failure.getCause(); }
            assertTrue(failure.getMessage().contains("missing/different completion history"));
            assertEquals(0, consumer.status().deliveredEvents());
        }
    }

    private static ConsumerSettings scenario() throws Exception {
        String topic = "consumer-" + UUID.randomUUID();
        KAFKA.createTopic(topic);
        return new ConsumerSettings(topic, "group-" + UUID.randomUUID(), Duration.ofSeconds(5),
                100, 1_000, 1_000_000, 1_024);
    }
    private static ActiveKafkaConsumer consumer(ConsumerSettings settings) {
        return new ActiveKafkaConsumer(settings, KAFKA.aProperties(), KAFKA.bProperties());
    }
    private static Event event(String id, byte value) { return new Event(id, "kafka", new byte[] {0, -1, value}); }
    private static void publish(ConsumerSettings settings, List<Event> events) throws Exception {
        try (var publisher = DualKafkaPublisher.open(PublishConfig.defaults(settings.topic()), KAFKA.aProperties(), KAFKA.bProperties())) {
            for (var event : events) {
                assertTrue(publisher.send(event).toCompletableFuture().get(10, TimeUnit.SECONDS).successful());
            }
        }
    }
    private static Batch awaitBatch(ActiveKafkaConsumer consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (System.nanoTime() - deadline < 0) {
            Batch batch = consumer.poll(Duration.ofMillis(200));
            if (!batch.isEmpty()) { return batch; }
        }
        throw new AssertionError("batch not delivered: " + consumer.status());
    }
    private static void awaitFiltered(ActiveKafkaConsumer consumer, int count) {
        long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
        while (consumer.status().filteredRecords() < count && System.nanoTime() - deadline < 0) {
            assertTrue(consumer.poll(Duration.ofMillis(200)).isEmpty(), "completed event reached handler");
        }
        assertTrue(consumer.status().filteredRecords() >= count, consumer.status()::toString);
    }
    private static long offset(ConsumerSettings settings) throws Exception {
        try (Admin admin = Admin.create(KAFKA.aProperties())) {
            var value = admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS).get(new TopicPartition(settings.topic(), 0));
            return value == null ? -1 : value.offset();
        }
    }
    private static void reset(ConsumerSettings settings, Map<String, Object> properties) throws Exception {
        try (var ledger = new CompletionLedger(settings, properties); Admin admin = Admin.create(properties)) {
            String metadata = "kaa-ledger=" + ledger.snapshot().topicId() + ";input=" + ledger.businessTopicId();
            admin.alterConsumerGroupOffsets(settings.groupId(), Map.of(
                    new TopicPartition(settings.topic(), 0), new OffsetAndMetadata(0, metadata),
                    new TopicPartition(settings.topic(), 1), new OffsetAndMetadata(0, metadata))).all().get(10, TimeUnit.SECONDS);
        }
    }
}

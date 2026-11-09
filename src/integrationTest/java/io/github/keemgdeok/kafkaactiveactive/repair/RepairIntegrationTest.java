package io.github.keemgdeok.kafkaactiveactive.repair;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;

@Testcontainers
@SuppressWarnings("try")
class RepairIntegrationTest {
    private static final String IMAGE = "apache/kafka@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7";
    @Container
    private static final KafkaContainer A = kafka("QaH9lFnjS0m1L8o7Yp3Wbg", 0);
    @Container
    private static final KafkaContainer B = kafka("PbK8mGojT1n2M9p8Zq4Xch", 0);

    private static KafkaContainer kafka(String clusterId, int hostPort) {
        KafkaContainer container = new KafkaContainer(IMAGE)
                .withEnv("CLUSTER_ID", clusterId)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
        if (hostPort > 0) {
            container.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(hostPort), new ExposedPort(9092))));
        }
        return container;
    }

    @AfterAll
    static void logs() throws IOException {
        saveLog("repair-a", A);
        saveLog("repair-b", B);
    }

    @Test
    void repairsBothDirectionsPreservesBytesAndRepeatedRunsDoNotGrowLogs() throws Exception {
        String topic = topic();
        var left = List.of(event("a-0", "kafka"), event("a-1", "hello"));
        var right = List.of(event("b-0", "kafka"), event("b-1", "hello"));
        write(A, topic, left);
        write(B, topic, right);
        try (var repair = service(topic)) {
            var report = repair.runOnce();
            assertEquals(4, report.uniqueEvents());
            assertEquals(2, report.copiedToA());
            assertEquals(2, report.copiedToB());
            for (int i = 0; i < 3; i++) {
                var repeated = repair.runOnce();
                assertEquals(0, repeated.copiedToA() + repeated.copiedToB());
            }
        }
        var expected = new ArrayList<>(left);
        expected.addAll(right);
        assertRecords(A, topic, expected, 4);
        assertRecords(B, topic, expected, 4);
    }

    @Test
    void payloadConflictFailsBeforeCopyingAnyMissingEvent() throws Exception {
        String topic = topic();
        write(A, topic, List.of(event("missing", "kafka"), new Event("conflict", "hello", new byte[] {1})));
        write(B, topic, List.of(new Event("conflict", "hello", new byte[] {2})));
        try (var repair = service(topic)) {
            assertReason(RepairException.Reason.CONFLICT, repair::runOnce);
            assertEquals(0, repair.status().lastFailure().copiedToA() + repair.status().lastFailure().copiedToB());
        }
        assertEquals(2, read(A, topic).size());
        assertEquals(1, read(B, topic).size());
    }

    @Test
    void indexLimitStopsBeforeWritingAndDoesNotSilentlySkipEvents() throws Exception {
        String topic = topic();
        write(A, topic, List.of(event("one", "kafka"), event("two", "hello")));
        var limited = new RepairConfig(topic, Duration.ofSeconds(30), Duration.ofSeconds(1), 1, 100_000, 1_024);
        try (var repair = new RepairService(limited, properties(A), properties(B))) {
            assertReason(RepairException.Reason.INDEX_LIMIT, repair::runOnce);
        }
        assertTrue(read(B, topic).isEmpty());
    }

    @Test
    void topicRecreationUsesNewIdentityInsteadOfOldOffsetsAndPresenceFlags() throws Exception {
        String topic = topic();
        var events = List.of(event("survivor", "kafka"));
        write(A, topic, events);
        try (var repair = service(topic)) {
            var before = repair.runOnce();
            try (Admin admin = Admin.create(properties(B))) {
                admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
            }
            create(B, topic);
            var after = repair.runOnce();
            assertNotEquals(before.clusterB().topicId(), after.clusterB().topicId());
            assertEquals(0, after.clusterB().partitions().get(0).end());
            assertEquals(1, after.copiedToB());
        }
        assertRecords(B, topic, events, 1);
    }

    @Test
    void replacedStorageAtTheSameAddressRebuildsFromTheSurvivingReplica() throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) {
            port = reservation.getLocalPort();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        String topic = topic();
        var events = List.of(event("replica-source", "kafka"), event("other-key", "hello"));
        write(A, topic, events);
        KafkaContainer original = kafka("AcG9lFnjS0m1L8o7Yp3Wbg", port);
        KafkaContainer replacement = kafka("BcK8mGojT1n2M9p8Zq4Xch", port);
        try {
            original.start();
            create(original, topic);
            var originalProperties = properties(original);
            try (var repair = new RepairService(RepairConfig.defaults(topic), properties(A), originalProperties)) {
                var before = repair.runOnce();
                assertEquals(2, before.copiedToB());
                saveLog("storage-before", original);
                original.stop(); // Remove the container and its ephemeral Kafka log storage.
                replacement.start();
                assertEquals(originalProperties, properties(replacement));
                create(replacement, topic);
                var after = repair.runOnce();
                assertNotEquals(before.clusterB().clusterId(), after.clusterB().clusterId());
                assertNotEquals(before.clusterB().topicId(), after.clusterB().topicId());
                assertEquals(2, after.copiedToB());
                assertEquals(0, repair.runOnce().copiedToB());
                assertRecords(replacement, topic, events, 2);
            }
        } finally {
            if (replacement.isRunning()) {
                saveLog("storage-after", replacement);
            }
            replacement.stop();
            original.stop();
        }
    }

    @Test
    void deletedBeginningAndAutomaticRetentionAreRejected() throws Exception {
        String deleted = topic();
        write(A, deleted, List.of(event("first", "kafka"), event("second", "kafka")));
        try (Admin admin = Admin.create(properties(A))) {
            admin.deleteRecords(Map.of(new TopicPartition(deleted, 0), RecordsToDelete.beforeOffset(1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
        try (var repair = service(deleted)) {
            assertReason(RepairException.Reason.LOG_CHANGED, repair::runOnce);
        }
        assertTrue(read(B, deleted).isEmpty());
        String retained = topic();
        try (Admin admin = Admin.create(properties(A))) {
            var resource = new org.apache.kafka.common.config.ConfigResource(
                    org.apache.kafka.common.config.ConfigResource.Type.TOPIC, retained);
            admin.incrementalAlterConfigs(Map.of(resource, List.of(new org.apache.kafka.clients.admin.AlterConfigOp(
                    new org.apache.kafka.clients.admin.ConfigEntry("retention.ms", "60000"),
                    org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET)))).all().get(10, TimeUnit.SECONDS);
        }
        try (var repair = service(retained)) {
            assertReason(RepairException.Reason.INVALID_TOPOLOGY, repair::runOnce);
        }
    }

    @Test
    void periodicRepairReportsFailureAndRecoversWhenBrokerReturns() throws Exception {
        String topic = topic();
        var events = List.of(event("one-copy", "kafka"));
        write(A, topic, events);
        B.getDockerClient().pauseContainerCmd(B.getContainerId()).exec();
        try {
            var config = new RepairConfig(topic, Duration.ofSeconds(20), Duration.ofMillis(100),
                    100, 100_000, 1_024);
            try (var repair = new RepairService(config, properties(A), properties(B))) {
                repair.start();
                repair.start();
                await(() -> repair.status().lastFailure() != null, Duration.ofSeconds(15));
                assertEquals(null, repair.status().lastCompleted());
                B.getDockerClient().unpauseContainerCmd(B.getContainerId()).exec();
                await(() -> repair.status().lastCompleted() != null, Duration.ofSeconds(20));
                String completed = repair.status().lastCompleted().runId();
                await(() -> !completed.equals(repair.status().lastCompleted().runId()), Duration.ofSeconds(20));
            }
        } finally {
            if (Boolean.TRUE.equals(B.getDockerClient().inspectContainerCmd(B.getContainerId()).exec().getState().getPaused())) {
                B.getDockerClient().unpauseContainerCmd(B.getContainerId()).exec();
            }
        }
        assertRecords(B, topic, events, 1);
    }

    @Test
    void finiteRepairCompletesAsSdkWritesAdvanceTheLiveTail() throws Exception {
        String topic = topic();
        write(A, topic, List.of(event("seed", "kafka")));
        AtomicInteger generated = new AtomicInteger();
        RepairReport report;
        try (var replication = ReplicationFixture.start(topic, properties(A), properties(B));
             var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(PublishConfig.defaults(topic), properties(A), properties(B)))) {
            var config = RepairConfig.defaults(topic);
            try (var repair = new RepairService(config,
                    deadline -> new AdvancingLog(new KafkaRepairLog(config, properties(A), deadline), publisher, generated),
                    deadline -> new AdvancingLog(new KafkaRepairLog(config, properties(B), deadline), publisher, generated),
                    System::nanoTime)) {
                report = repair.runOnce();
            }
        }
        assertTrue(generated.get() > 0);
        assertTrue(report.uniqueEvents() < generated.get() + 1, "live tail must remain beyond the finite boundary");
        try (var repair = service(topic)) {
            repair.runOnce();
            var repeated = repair.runOnce();
            assertEquals(0, repeated.copiedToA() + repeated.copiedToB());
        }
        List<Event> expected = new ArrayList<>(List.of(event("seed", "kafka")));
        for (int i = 0; i < generated.get(); i++) {
            expected.add(event("live-" + i, "hello"));
        }
        assertRecords(A, topic, expected, -1);
        assertRecords(B, topic, expected, -1);
    }

    private static RepairService service(String topic) {
        return new RepairService(RepairConfig.defaults(topic), properties(A), properties(B));
    }

    private static Map<String, Object> properties(KafkaContainer broker) {
        return Map.of("bootstrap.servers", broker.getBootstrapServers());
    }

    private static String topic() throws Exception {
        String topic = "repair-" + UUID.randomUUID();
        create(A, topic);
        create(B, topic);
        return topic;
    }

    private static void create(KafkaContainer broker, String topic) throws Exception {
        try (Admin admin = Admin.create(properties(broker))) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                    "retention.ms", "-1", "retention.bytes", "-1", "cleanup.policy", "delete"))))
                    .all().get(15, TimeUnit.SECONDS);
        }
    }

    private static Event event(String id, String key) {
        return new Event(id, key, ("payload:" + id).getBytes(StandardCharsets.UTF_8));
    }

    private static void write(KafkaContainer broker, String topic, List<Event> events) throws Exception {
        Map<String, Object> properties = new HashMap<>(properties(broker));
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        properties.put("acks", "all");
        try (var producer = new KafkaProducer<byte[], byte[]>(properties)) {
            for (Event event : events) {
                var record = new ProducerRecord<>(topic, event.partition(), null,
                        event.key().getBytes(StandardCharsets.UTF_8), event.payload(), List.of(
                        new RecordHeader(RoutedKafkaPublisher.EVENT_ID_HEADER, event.eventId().getBytes(StandardCharsets.UTF_8)),
                        new RecordHeader(RoutedKafkaPublisher.FORMAT_HEADER, new byte[] {1})));
                producer.send(record).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static List<ConsumerRecord<byte[], byte[]>> read(KafkaContainer broker, String topic) {
        Map<String, Object> properties = new HashMap<>(properties(broker));
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", false);
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            var partitions = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var ends = consumer.endOffsets(partitions, Duration.ofSeconds(10));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < ends.get(p)) && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            }
            assertTrue(partitions.stream().allMatch(p -> consumer.position(p) >= ends.get(p)));
        }
        return records;
    }

    private static void assertRecords(KafkaContainer broker, String topic, List<Event> expected, int physicalCount) {
        var records = read(broker, topic);
        if (physicalCount >= 0) {
            assertEquals(physicalCount, records.size());
        }
        Map<String, Event> byId = new HashMap<>();
        for (Event event : expected) {
            byId.put(event.eventId(), event);
        }
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (var record : records) {
            var decoded = RepairIndex.decode(record, 900_000);
            Event event = byId.get(decoded.id());
            assertTrue(event != null, "unexpected event ID");
            assertEquals(event.partition(), record.partition());
            assertArrayEquals(event.key().getBytes(StandardCharsets.UTF_8), record.key());
            assertArrayEquals(event.payload(), record.value());
            seen.add(decoded.id());
        }
        assertEquals(byId.keySet(), seen);
    }

    private static void assertReason(RepairException.Reason reason, Runnable operation) {
        assertEquals(reason, assertThrows(RepairException.class, operation::run).reason());
    }

    private static void await(BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(Duration.ofMillis(20).toNanos());
        }
        assertTrue(condition.getAsBoolean(), "condition was not reached before deadline");
    }

    private static void saveLog(String name, KafkaContainer container) throws IOException {
        Path directory = Path.of("build", "reports", "integrationTest", "repair-brokers");
        Files.createDirectories(directory);
        Files.writeString(directory.resolve(name + ".log"), container.getLogs());
    }

    private record AdvancingLog(RepairLog delegate, RoutedKafkaPublisher publisher, AtomicInteger generated)
            implements RepairLog {
        @Override
        public RepairReport.LogBoundary capture() {
            var snapshot = delegate.capture();
            var result = publisher.send(event("live-" + generated.getAndIncrement(), "hello"))
                    .toCompletableFuture().join();
            assertTrue(result.successful());
            return snapshot;
        }

        @Override
        public void scan(RepairReport.LogBoundary boundary, Map<Integer, Long> from,
                          Consumer<ConsumerRecord<byte[], byte[]>> visitor) {
            delegate.scan(boundary, from, visitor);
        }

        @Override
        public void append(ConsumerRecord<byte[], byte[]> record) { delegate.append(record); }
        @Override
        public void close() { delegate.close(); }
    }
}

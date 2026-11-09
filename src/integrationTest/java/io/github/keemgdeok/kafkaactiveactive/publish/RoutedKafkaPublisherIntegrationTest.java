package io.github.keemgdeok.kafkaactiveactive.publish;

import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.ACCEPTED_DEGRADED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.CONFIRMED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
@SuppressWarnings("try")
class RoutedKafkaPublisherIntegrationTest {
    // apache/kafka:4.3.2 multi-platform manifest; use the digest form supported by Testcontainers.
    private static final String IMAGE = "apache/kafka@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7";
    @Container
    private static final KafkaContainer A = kafka("QaH9lFnjS0m1L8o7Yp3Wbg");
    @Container
    private static final KafkaContainer B = kafka("PbK8mGojT1n2M9p8Zq4Xch");

    private static KafkaContainer kafka(String clusterId) {
        // Docker may reallocate anonymous host ports on stop/start. Keep the advertised listener stable.
        int hostPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            hostPort = reservation.getLocalPort();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        return new KafkaContainer(IMAGE)
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(hostPort), new ExposedPort(9092))))
                .withEnv("CLUSTER_ID", clusterId)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
    }

    @AfterAll
    static void logs() throws IOException {
        Path directory = Path.of("build", "reports", "integrationTest", "publisher-brokers");
        Files.createDirectories(directory);
        Files.writeString(directory.resolve("kafka-a.log"), A.getLogs());
        Files.writeString(directory.resolve("kafka-b.log"), B.getLogs());
    }

    @Test
    void bothLogsContainIdenticalIdsKeysPayloadsAndPerKeyOrder() throws Exception {
        String topic = createTopic();
        List<Event> events = events(40);
        List<PublishResult> results;
        try (var replication = ReplicationFixture.start(topic, properties(A), properties(B));
             var publisher = ReplicationFixture.ready(open(topic))) {
            results = publish(publisher, events);
            assertTrue(results.stream().allMatch(r -> r.status() == CONFIRMED));
        }
        Map<String, ConsumerRecord<byte[], byte[]>> a = read(A, topic, events.size());
        Map<String, ConsumerRecord<byte[], byte[]>> b = read(B, topic, events.size());
        assertEquals(a.keySet(), b.keySet());
        for (Event event : events) {
            var left = a.get(event.eventId());
            var right = b.get(event.eventId());
            assertEquals(event.partition(), left.partition());
            assertEquals(left.partition(), right.partition());
            assertArrayEquals(event.key().getBytes(StandardCharsets.UTF_8), left.key());
            assertArrayEquals(event.payload(), left.value());
            assertArrayEquals(left.key(), right.key());
            assertArrayEquals(left.value(), right.value());
            assertArrayEquals(new byte[] {2}, left.headers().lastHeader(RoutedKafkaPublisher.FORMAT_HEADER).value());
        }
        for (var log : List.of(a, b)) {
            Map<String, Long> previous = new HashMap<>();
            for (Event event : events) {
                long offset = log.get(event.eventId()).offset();
                assertTrue(offset > previous.getOrDefault(event.key(), -1L));
                previous.put(event.key(), offset);
            }
        }
        for (PublishResult result : results) {
            assertEquals(a.get(result.eventId()).offset(), result.clusterA().offset());
            assertEquals(b.get(result.eventId()).offset(), result.clusterB().offset());
        }
    }

    @Test
    void withoutReplicationOnlyPreferredRawExistsAndResumeCopiesSamePartitionWithoutLooping() throws Exception {
        String topic = createTopic();
        var events = events(2);
        List<PublishResult> results;
        try (var publisher = open(topic)) {
            results = publish(publisher, events);
            assertTrue(results.stream().allMatch(r -> r.status() == ACCEPTED_DEGRADED));
        }
        var rawA = read(A, topic, 1); var rawB = read(B, topic, 1);
        assertEquals(events.get(0).eventId(), rawA.keySet().iterator().next());
        assertEquals(events.get(1).eventId(), rawB.keySet().iterator().next());
        assertEquals(EventCodec.Kind.RAW, EventCodec.kind(rawA.values().iterator().next()));
        assertEquals(EventCodec.Kind.RAW, EventCodec.kind(rawB.values().iterator().next()));
        try (var replication = ReplicationFixture.start(topic, properties(A), properties(B))) {
            long end = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while ((replication.status().aToB().replicatedRecords() < 1
                    || replication.status().bToA().replicatedRecords() < 1) && System.nanoTime() < end) {
                java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            }
            assertEquals(1, replication.status().aToB().replicatedRecords());
            assertEquals(1, replication.status().bToA().replicatedRecords());
            var allA = read(A, topic, 2); var allB = read(B, topic, 2);
            for (var event : events) {
                assertEquals(event.partition(), allA.get(event.eventId()).partition());
                assertEquals(event.partition(), allB.get(event.eventId()).partition());
                assertEquals(event.partition() == 0 ? EventCodec.Kind.RAW : EventCodec.Kind.REPLICA,
                        EventCodec.kind(allA.get(event.eventId())));
                assertEquals(event.partition() == 1 ? EventCodec.Kind.RAW : EventCodec.Kind.REPLICA,
                        EventCodec.kind(allB.get(event.eventId())));
            }
        }
        assertTrue(results.stream().allMatch(r -> r.status() == ACCEPTED_DEGRADED), "returned evidence is immutable");
    }

    @Test
    void survivingBrokerAcceptsAllKeysEvenWhenPublisherStartsDuringEitherOutage() throws Exception {
        for (KafkaContainer failed : List.of(A, B)) {
            String topic = createTopic();
            KafkaContainer survivor = failed == A ? B : A;
            List<Event> events = events(8);
            assertEquals(2, events.stream().map(Event::partition).distinct().count());
            failed.getDockerClient().stopContainerCmd(failed.getContainerId()).withTimeout(5).exec();
            try {
                try (var publisher = open(topic)) {
                    var results = publish(publisher, events);
                    assertTrue(results.stream().allMatch(r -> r.status() == ACCEPTED_DEGRADED), results::toString);
                }
                var log = read(survivor, topic, events.size());
                for (Event event : events) {
                    assertArrayEquals(event.payload(), log.get(event.eventId()).value());
                    assertEquals(event.partition(), log.get(event.eventId()).partition());
                }
            } finally {
                failed.getDockerClient().startContainerCmd(failed.getContainerId()).exec();
                awaitBroker(failed);
            }
        }
    }

    @Test
    void runningPublisherContinuesWhenBrokerStopsAndConfirmsAfterItReturns() throws Exception {
        String topic = createTopic();
        try (var replication = ReplicationFixture.start(topic, properties(A), properties(B));
             var publisher = ReplicationFixture.ready(open(topic))) {
            assertEquals(CONFIRMED, publisher.send(events(1).getFirst()).toCompletableFuture()
                    .get(15, TimeUnit.SECONDS).status());
            A.getDockerClient().stopContainerCmd(A.getContainerId()).withTimeout(5).exec();
            try {
                Event event = new Event("during-outage", "kafka", new byte[] {42});
                var result = publisher.send(event).toCompletableFuture().get(15, TimeUnit.SECONDS);
                assertEquals(ACCEPTED_DEGRADED, result.status());
                assertArrayEquals(event.payload(), read(B, topic, 2).get(event.eventId()).value());
            } finally {
                A.getDockerClient().startContainerCmd(A.getContainerId()).exec();
                awaitBroker(A);
            }
            long readyBy = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < readyBy && (!publisher.status().clusterAConfirmationReady()
                    || !publisher.status().clusterBConfirmationReady()
                    || replication.status().aToB().state() != io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService.State.RUNNING
                    || replication.status().bToA().state() != io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService.State.RUNNING)) {
                java.util.concurrent.locks.LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            }
            assertTrue(publisher.status().clusterAConfirmationReady() && publisher.status().clusterBConfirmationReady());
            assertEquals(io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService.State.RUNNING, replication.status().aToB().state());
            assertEquals(io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService.State.RUNNING, replication.status().bToA().state());
            Event recovered = new Event("after-recovery", "hello", new byte[] {43});
            assertEquals(CONFIRMED, publisher.send(recovered).toCompletableFuture()
                    .get(15, TimeUnit.SECONDS).status());
        }
    }

    private static Map<String, Object> properties(KafkaContainer broker) { return Map.of("bootstrap.servers", broker.getBootstrapServers()); }

    private static RoutedKafkaPublisher open(String topic) {
        return RoutedKafkaPublisher.open(new PublishConfig(topic, Duration.ofSeconds(8), 64, 900_000,
                Duration.ofSeconds(2)), Map.of("bootstrap.servers", A.getBootstrapServers()),
                Map.of("bootstrap.servers", B.getBootstrapServers()));
    }

    @Test
    void noReachableBrokerReturnsUnknownRatherThanFalseSuccessOrRejection() throws Exception {
        String topic = createTopic();
        for (KafkaContainer broker : List.of(A, B)) {
            broker.getDockerClient().stopContainerCmd(broker.getContainerId()).withTimeout(5).exec();
        }
        try (var publisher = open(topic)) {
            var result = publisher.send(events(1).getFirst()).toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertEquals(PublishResult.Status.UNKNOWN, result.status());
        } finally {
            for (KafkaContainer broker : List.of(A, B)) {
                broker.getDockerClient().startContainerCmd(broker.getContainerId()).exec();
            }
            for (KafkaContainer broker : List.of(A, B)) {
                awaitBroker(broker);
            }
        }
    }

    private static List<Event> events(int count) {
        List<Event> events = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            events.add(new Event("event-" + i, i % 2 == 0 ? "kafka" : "hello",
                    ("payload-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        return events;
    }

    private static List<PublishResult> publish(RoutedKafkaPublisher publisher, List<Event> events) throws Exception {
        List<CompletableFuture<PublishResult>> futures = events.stream()
                .map(event -> publisher.send(event).toCompletableFuture()).toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(15, TimeUnit.SECONDS);
        return futures.stream().map(CompletableFuture::join).toList();
    }

    private static String createTopic() throws Exception {
        String topic = "publish-" + UUID.randomUUID();
        for (KafkaContainer broker : List.of(A, B)) {
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of("retention.ms", "-1", "retention.bytes", "-1"))))
                        .all().get(20, TimeUnit.SECONDS);
            }
        }
        return topic;
    }

    private static void awaitBroker(KafkaContainer broker) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", broker.getBootstrapServers(),
                "default.api.timeout.ms", 30_000))) {
            admin.describeCluster().clusterId().get(30, TimeUnit.SECONDS);
        }
    }

    private static Map<String, ConsumerRecord<byte[], byte[]>> read(KafkaContainer broker, String topic,
                                                                  int expected) {
        Map<String, Object> properties = Map.of("bootstrap.servers", broker.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Map<String, ConsumerRecord<byte[], byte[]>> records = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> partitions = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions, Duration.ofSeconds(10));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < ends.get(p))
                    && System.nanoTime() - deadline < 0) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    String id = new String(record.headers().lastHeader(RoutedKafkaPublisher.EVENT_ID_HEADER).value(),
                            StandardCharsets.UTF_8);
                    assertEquals(null, records.put(id, record), "unexpected duplicate event ID");
                }
            }
            assertTrue(partitions.stream().allMatch(p -> consumer.position(p) >= ends.get(p)),
                    "did not reach the finite log boundary");
        }
        assertEquals(expected, records.size());
        return records;
    }
}

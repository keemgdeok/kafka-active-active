package io.github.keemgdeok.kafkaactiveactive.environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class IndependentKafkaTest {
    // apache/kafka:4.3.2, pinned to the multi-platform manifest.
    private static final String IMAGE = "apache/kafka@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7";
    private static final String TOPIC = "events";

    @Container
    private static final KafkaContainer A = kafka("QaH9lFnjS0m1L8o7Yp3Wbg");

    @Container
    private static final KafkaContainer B = kafka("PbK8mGojT1n2M9p8Zq4Xch");

    private static KafkaContainer kafka(String clusterId) {
        return new KafkaContainer(IMAGE)
                .withEnv("CLUSTER_ID", clusterId)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
    }

    @BeforeAll
    static void createTopics() throws Exception {
        for (KafkaContainer kafka : List.of(A, B)) {
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1)))
                        .all().get(20, TimeUnit.SECONDS);
            }
        }
    }

    @AfterAll
    static void collectBrokerLogs() throws IOException {
        Path logs = Path.of("build", "reports", "integrationTest", "brokers");
        Files.createDirectories(logs);
        Files.writeString(logs.resolve("kafka-a.log"), A.getLogs());
        Files.writeString(logs.resolve("kafka-b.log"), B.getLogs());
    }

    @Test
    void clustersHaveIndependentIdentities() throws Exception {
        var topology = KafkaTopologyVerifier.verify(
                A.getBootstrapServers(), B.getBootstrapServers(), Duration.ofSeconds(10));
        assertNotEquals(topology.clusterAId(), topology.clusterBId());
    }

    @Test
    void rejectsTwoAddressesFromTheSameCluster() {
        assertThrows(IllegalArgumentException.class, () -> KafkaTopologyVerifier.verify(
                A.getBootstrapServers(), A.getBootstrapServers(), Duration.ofSeconds(10)));
    }

    @Test
    void eachClusterHasTwoSingleReplicaPartitions() throws Exception {
        for (KafkaContainer kafka : List.of(A, B)) {
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
                var topic = admin.describeTopics(List.of(TOPIC)).allTopicNames()
                        .get(10, TimeUnit.SECONDS).get(TOPIC);
                assertEquals(2, topic.partitions().size());
                assertTrue(topic.partitions().stream().allMatch(p -> p.replicas().size() == 1));
            }
        }
    }

    @Test
    void eachClusterRoundTripsItsOwnRecords() throws Exception {
        roundTrip(A, 0, "a-only");
        roundTrip(B, 1, "b-only");
    }

    private static void roundTrip(KafkaContainer kafka, int partition, String value) throws Exception {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        producerProperties.put("acks", "all");
        producerProperties.put("enable.idempotence", "true");
        producerProperties.put("delivery.timeout.ms", "10000");
        producerProperties.put("request.timeout.ms", "5000");
        long offset;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            offset = producer.send(new ProducerRecord<>(TOPIC, partition, "key", value))
                    .get(15, TimeUnit.SECONDS).offset();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("enable.auto.commit", "false");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            TopicPartition topicPartition = new TopicPartition(TOPIC, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seek(topicPartition, offset);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() == offset) {
                        assertEquals(value, record.value());
                        return;
                    }
                }
            }
            throw new AssertionError("Record was not read before the deadline: " + value);
        }
    }
}

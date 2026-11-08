package io.github.keemgdeok.kafkaactiveactive.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.kafka.KafkaContainer;

/** Shared real-Kafka fixture for consumer/group/fault verification. Docker absence is a failure. */
public final class KafkaPair implements AutoCloseable {
    public static final String IMAGE = "apache/kafka@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7";
    public final KafkaContainer a = broker("QaH9lFnjS0m1L8o7Yp3Wbg");
    public final KafkaContainer b = broker("PbK8mGojT1n2M9p8Zq4Xch");
    private final String diagnostics;
    public KafkaPair(String diagnostics) { this.diagnostics = diagnostics; }
    public static KafkaContainer broker(String id) {
        return new KafkaContainer(IMAGE).withEnv("CLUSTER_ID", id)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
    }
    public void start() { a.start(); b.start(); }
    public Map<String, Object> aProperties() { return Map.of("bootstrap.servers", a.getBootstrapServers()); }
    public Map<String, Object> bProperties() { return Map.of("bootstrap.servers", b.getBootstrapServers()); }
    public void createTopic(String topic) throws Exception {
        for (var properties : List.of(aProperties(), bProperties())) {
            try (Admin admin = Admin.create(properties)) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                        "retention.ms", "-1", "retention.bytes", "-1", "cleanup.policy", "delete"))))
                        .all().get(15, TimeUnit.SECONDS);
            }
        }
    }
    @Override public void close() throws IOException {
        try {
            Path path = Path.of("build", "reports", "integrationTest", diagnostics);
            Files.createDirectories(path);
            if (a.isRunning()) { Files.writeString(path.resolve("kafka-a.log"), a.getLogs()); }
            if (b.isRunning()) { Files.writeString(path.resolve("kafka-b.log"), b.getLogs()); }
        } finally {
            try { a.stop(); } finally { b.stop(); }
        }
    }
}

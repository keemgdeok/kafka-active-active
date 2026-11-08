package io.github.keemgdeok.kafkaactiveactive.baseline;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.MinimumDurationRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.images.builder.Transferable;

/** Two independent brokers plus an optional MM2 worker; the worker is not a third Kafka node. */
final class MirrorEnvironment implements AutoCloseable {
    private final Network network = Network.newNetwork();
    final KafkaContainer a = broker("QaH9lFnjS0m1L8o7Yp3Wbg", "a");
    final KafkaContainer b = broker("PbK8mGojT1n2M9p8Zq4Xch", "b");
    private GenericContainer<?> mirror;
    private KafkaContainer broker(String id, String alias) {
        return new KafkaContainer(KafkaPair.IMAGE).withNetwork(network).withNetworkAliases(alias)
                .withListener(alias + ":19092").withEnv("CLUSTER_ID", id)
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
    }
    void start() { a.start(); b.start(); }
    Map<String, Object> aProperties() { return Map.of("bootstrap.servers", a.getBootstrapServers()); }
    Map<String, Object> bProperties() { return Map.of("bootstrap.servers", b.getBootstrapServers()); }
    void createTopic(String topic) throws Exception {
        for (var properties : List.of(aProperties(), bProperties())) {
            try (var admin = Admin.create(properties)) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                        "retention.ms", "-1", "retention.bytes", "-1", "cleanup.policy", "delete"))))
                        .all().get(10, TimeUnit.SECONDS);
            }
        }
    }
    void startMirror(String topic) throws IOException {
        String config;
        try (var resource = MirrorEnvironment.class.getResourceAsStream("/mm2.properties")) {
            if (resource == null) { throw new IOException("missing MM2 configuration"); }
            config = new String(resource.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).replace("@TOPIC@", topic);
        }
        Files.createDirectories(Path.of("build/reports/baseline"));
        Files.writeString(Path.of("build/reports/baseline/mm2.properties"), config);
        mirror = new GenericContainer<>(KafkaPair.IMAGE).withNetwork(network)
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
                .withCopyToContainer(Transferable.of(config), "/tmp/mm2.properties")
                .withCreateContainerCmdModifier(command -> command.withEntrypoint("/bin/sh"))
                .withCommand("-c", "exec /opt/kafka/bin/connect-mirror-maker.sh /tmp/mm2.properties")
                .waitingFor(Wait.forSuccessfulCommand("kill -0 1"))
                .withStartupCheckStrategy(new MinimumDurationRunningStartupCheckStrategy(Duration.ofSeconds(2)));
        mirror.start(); // Readiness is proven by finite event-set/checkpoint conditions, not this process check.
    }
    void pauseA() { a.getDockerClient().pauseContainerCmd(a.getContainerId()).exec(); }
    void resumeA() { a.getDockerClient().unpauseContainerCmd(a.getContainerId()).exec(); }
    @Override public void close() throws IOException {
        try {
            Path path = Path.of("build/reports/baseline"); Files.createDirectories(path);
            if (a.isRunning()) { Files.writeString(path.resolve("kafka-a.log"), a.getLogs()); }
            if (b.isRunning()) { Files.writeString(path.resolve("kafka-b.log"), b.getLogs()); }
            if (mirror != null && mirror.isRunning()) { Files.writeString(path.resolve("mm2.log"), mirror.getLogs()); }
        } finally {
            try { if (mirror != null) { mirror.stop(); } }
            finally { try { a.stop(); } finally { try { b.stop(); } finally { network.close(); } } }
        }
    }
}

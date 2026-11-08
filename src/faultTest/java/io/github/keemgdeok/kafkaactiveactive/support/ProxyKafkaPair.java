package io.github.keemgdeok.kafkaactiveactive.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.Network;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Proxies both bootstrap and advertised addresses, so Kafka metadata cannot bypass the fault. */
public final class ProxyKafkaPair implements AutoCloseable {
    private final Network network = Network.newNetwork();
    private final ToxiproxyContainer proxy = new ToxiproxyContainer(
            "ghcr.io/shopify/toxiproxy@sha256:9378ed52a28bc50edc1350f936f518f31fa95f0d15917d6eb40b8e376d1a214e").withNetwork(network);
    public final KafkaContainer a = broker("QaH9lFnjS0m1L8o7Yp3Wbg", "a", 8666);
    public final KafkaContainer b = broker("PbK8mGojT1n2M9p8Zq4Xch", "b", 8667);
    public Proxy proxyA;
    public Proxy proxyB;
    private KafkaContainer broker(String id, String alias, int port) {
        return new KafkaContainer(KafkaPair.IMAGE).withNetwork(network).withNetworkAliases(alias)
                .withListener(alias + ":19092", () -> proxy.getHost() + ":" + proxy.getMappedPort(port))
                .withEnv("CLUSTER_ID", id).withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
    }
    public void start() throws IOException {
        proxy.start();
        var control = new ToxiproxyClient(proxy.getHost(), proxy.getControlPort());
        proxyA = control.createProxy("a", "0.0.0.0:8666", "a:19092");
        proxyB = control.createProxy("b", "0.0.0.0:8667", "b:19092");
        a.start(); b.start();
    }
    public Map<String, Object> aDirect() { return Map.of("bootstrap.servers", a.getBootstrapServers()); }
    public Map<String, Object> bDirect() { return Map.of("bootstrap.servers", b.getBootstrapServers()); }
    public Map<String, Object> aProxied() { return Map.of("bootstrap.servers", proxy.getHost() + ":" + proxy.getMappedPort(8666)); }
    public Map<String, Object> bProxied() { return Map.of("bootstrap.servers", proxy.getHost() + ":" + proxy.getMappedPort(8667)); }
    public void createTopic(String topic) throws Exception {
        for (var properties : List.of(aDirect(), bDirect())) {
            try (var admin = Admin.create(properties)) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                        "retention.ms", "-1", "retention.bytes", "-1", "cleanup.policy", "delete"))))
                        .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
            }
        }
    }
    public void heal() throws IOException {
        for (var connection : List.of(proxyA, proxyB)) {
            connection.enable();
            for (var toxic : connection.toxics().getAll()) { toxic.remove(); }
        }
    }
    @Override public void close() throws IOException {
        try {
            Path path = Path.of("build/reports/faultTest/network-brokers"); Files.createDirectories(path);
            if (a.isRunning()) { Files.writeString(path.resolve("kafka-a.log"), a.getLogs()); }
            if (b.isRunning()) { Files.writeString(path.resolve("kafka-b.log"), b.getLogs()); }
            if (proxy.isRunning()) { Files.writeString(path.resolve("toxiproxy.log"), proxy.getLogs()); }
        } finally {
            try { a.stop(); } finally { try { b.stop(); } finally { try { proxy.stop(); } finally { network.close(); } } }
        }
    }
}

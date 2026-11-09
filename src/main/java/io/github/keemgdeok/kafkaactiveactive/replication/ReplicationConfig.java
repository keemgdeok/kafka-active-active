package io.github.keemgdeok.kafkaactiveactive.replication;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;

public record ReplicationConfig(String topic, Duration operationTimeout, int maxBatchRecords,
                                int maxEventBytes, Duration closeTimeout) {
    public ReplicationConfig {
        new PublishConfig(topic, operationTimeout, 1, maxEventBytes, closeTimeout);
        if (operationTimeout.compareTo(Duration.ofSeconds(1)) < 0
                || operationTimeout.compareTo(Duration.ofSeconds(30)) > 0
                || maxBatchRecords < 1 || maxBatchRecords > 1_000) {
            throw new IllegalArgumentException("operation timeout must be 1..30s and batch size 1..1000");
        }
    }
    public static ReplicationConfig defaults(String topic) {
        return new ReplicationConfig(topic, Duration.ofSeconds(5), 100, 900_000, Duration.ofSeconds(5));
    }
    public String confirmationTopic() { return "__kaa_replication_ack_" + digest(topic); }
    public String groupId(LogIdentity source, LogIdentity target) {
        return "kaa-repl-" + generation(source, target);
    }
    public String transactionalId(LogIdentity source, LogIdentity target) {
        return "kaa-repl-tx-" + generation(source, target);
    }
    private String generation(LogIdentity source, LogIdentity target) {
        if (source.clusterId().equals(target.clusterId())) {
            throw new IllegalArgumentException("replication requires independent Kafka clusters");
        }
        return digest(topic + "\u0000" + source.clusterId() + "\u0000" + source.topicId()
                + "\u0000" + target.clusterId() + "\u0000" + target.topicId());
    }
    private static String digest(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}

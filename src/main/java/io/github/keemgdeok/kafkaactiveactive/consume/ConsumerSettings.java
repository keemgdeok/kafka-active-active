package io.github.keemgdeok.kafkaactiveactive.consume;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;

import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;

public record ConsumerSettings(String topic, String groupId, Duration operationTimeout,
                               int maxBatchRecords, int maxCompletedEntries, long maxCompletedBytes,
                               int maxEventBytes) {
    public ConsumerSettings {
        new PublishConfig(topic, operationTimeout, 1, maxEventBytes, operationTimeout);
        if (groupId == null || groupId.isBlank() || groupId.length() > 200
                || !StandardCharsets.UTF_8.newEncoder().canEncode(groupId)
                || operationTimeout.compareTo(Duration.ofSeconds(1)) < 0
                || operationTimeout.compareTo(Duration.ofSeconds(30)) > 0
                || maxBatchRecords < 1 || maxBatchRecords > 1_000
                || maxCompletedEntries < 1 || maxCompletedBytes < 1) {
            throw new IllegalArgumentException("invalid consumer group, bounds or operation timeout (1..30s)");
        }
    }

    public static ConsumerSettings defaults(String topic, String groupId) {
        return new ConsumerSettings(topic, groupId, Duration.ofSeconds(10), 100,
                1_000_000, 256L * 1024 * 1024, 900_000);
    }

    public String completionTopic() { return "__kaa_completed_" + namespace(); }
    public String transactionalId(int partition) {
        if (partition < 0 || partition > 1) { throw new IllegalArgumentException("invalid partition"); }
        return "kaa-" + namespace() + "-" + partition;
    }

    private String namespace() {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest((groupId + "\u0000" + topic).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}

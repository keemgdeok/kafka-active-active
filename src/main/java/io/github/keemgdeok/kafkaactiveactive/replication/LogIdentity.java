package io.github.keemgdeok.kafkaactiveactive.replication;

import java.util.Objects;
import java.nio.charset.StandardCharsets;

/** Native identities, never bootstrap addresses or logical A/B labels. */
public record LogIdentity(String clusterId, String topicId) {
    public LogIdentity {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(topicId, "topicId");
        if (clusterId.isBlank() || topicId.isBlank()
                || !StandardCharsets.UTF_8.newEncoder().canEncode(clusterId)
                || !StandardCharsets.UTF_8.newEncoder().canEncode(topicId)
                || clusterId.getBytes(StandardCharsets.UTF_8).length > 256
                || topicId.getBytes(StandardCharsets.UTF_8).length > 256) {
            throw new IllegalArgumentException("invalid native log identity");
        }
    }
}

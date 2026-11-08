package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.Set;

/**
 * Grades apply to newly completed IDs; input offsets remain local to the active Kafka.
 * UNKNOWN requires close/reopen: local transactions may have committed before a response was lost.
 */
public record CommitResult(Status status, Set<String> locallyCompletedIds, Set<String> peerConfirmedIds) {
    public CommitResult {
        locallyCompletedIds = Set.copyOf(locallyCompletedIds);
        peerConfirmedIds = Set.copyOf(peerConfirmedIds);
    }
    public enum Status { CONFIRMED, LOCAL_ONLY, UNKNOWN }
}

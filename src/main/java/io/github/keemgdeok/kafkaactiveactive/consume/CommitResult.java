package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.Set;

public record CommitResult(Status status, Set<String> locallyCompletedIds, Set<String> peerConfirmedIds) {
    public CommitResult {
        locallyCompletedIds = Set.copyOf(locallyCompletedIds);
        peerConfirmedIds = Set.copyOf(peerConfirmedIds);
    }
    public enum Status { CONFIRMED, LOCAL_ONLY, UNKNOWN }
}

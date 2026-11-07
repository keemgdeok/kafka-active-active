package io.github.keemgdeok.kafkaactiveactive.repair;

import java.time.Instant;
import java.util.Map;

/** Covers the captured finite boundaries, not subsequent writes to the live tail. */
public record RepairReport(String runId, Instant startedAt, Instant completedAt,
                           LogBoundary clusterA, LogBoundary clusterB,
                           int uniqueEvents, long indexedRecords, long estimatedIndexBytes,
                           long copiedToA, long copiedToB) {
    public record PartitionBoundary(long beginning, long end, int leaderEpoch) {}

    public record LogBoundary(String clusterId, String topicId, Map<Integer, PartitionBoundary> partitions) {
        public LogBoundary {
            partitions = Map.copyOf(partitions);
        }

        void requireContinuation(LogBoundary current) {
            if (!clusterId.equals(current.clusterId) || !topicId.equals(current.topicId)
                    || !partitions.keySet().equals(current.partitions.keySet())) {
                throw new RepairException(RepairException.Reason.LOG_CHANGED, "cluster or topic identity changed");
            }
            for (var entry : partitions.entrySet()) {
                PartitionBoundary before = entry.getValue();
                PartitionBoundary after = current.partitions.get(entry.getKey());
                if (after.beginning() != before.beginning() || after.end() < before.end()
                        || after.leaderEpoch() != before.leaderEpoch()) {
                    throw new RepairException(RepairException.Reason.LOG_CHANGED,
                            "log boundary or leader epoch changed for partition " + entry.getKey());
                }
            }
        }
    }
}

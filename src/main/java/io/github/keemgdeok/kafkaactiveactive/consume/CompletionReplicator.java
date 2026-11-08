package io.github.keemgdeok.kafkaactiveactive.consume;

import java.time.Duration;
import java.util.Map;

/** One reconciliation owner per stopped logical group. Sources and compacted replicas are equally valid. */
public final class CompletionReplicator {
    private final ConsumerSettings settings;
    private final Map<String, Object> clusterA;
    private final Map<String, Object> clusterB;
    public CompletionReplicator(ConsumerSettings settings, Map<String, Object> clusterA, Map<String, Object> clusterB) {
        this.settings = settings;
        this.clusterA = Map.copyOf(clusterA);
        this.clusterB = Map.copyOf(clusterB);
    }
    public synchronized Report runOnce() {
        long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
        try (var a = new CompletionLedger(settings, clusterA); var b = new CompletionLedger(settings, clusterB)) {
            a.requireGroupStopped();
            b.requireGroupStopped();
            // Resolve orphan transactions before snapshotting: empty membership alone does not
            // settle a transaction whose commit response was lost when the old worker stopped.
            a.fenceStoppedGroup();
            b.fenceStoppedGroup();
            var boundaryA = a.snapshot();
            var boundaryB = b.snapshot();
            if (boundaryA.clusterId().equals(boundaryB.clusterId())) {
                throw new IllegalArgumentException("completion replication requires independent clusters");
            }
            var indexA = a.replay(boundaryA);
            var indexB = b.replay(boundaryB);
            var union = new CompletedIndex(settings);
            indexA.markers().forEach(union::add);
            indexB.markers().forEach(union::add);
            long copiedA = 0;
            long copiedB = 0;
            for (var marker : union.markers()) {
                requireTime(deadline);
                if (!indexA.contains(marker)) { a.append(marker); copiedA++; }
                if (!indexB.contains(marker)) { b.append(marker); copiedB++; }
            }
            a.verify(boundaryA);
            b.verify(boundaryB);
            requireTime(deadline);
            return new Report(union.size(), copiedA, copiedB, boundaryA.topicId(), boundaryB.topicId());
        }
    }
    private static void requireTime(long deadline) {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) {
            throw new IllegalStateException("completion reconciliation interrupted or its 5 minute deadline elapsed");
        }
    }
    public record Report(int completedIds, long copiedToA, long copiedToB, String topicIdA, String topicIdB) {}
}

package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

import io.github.keemgdeok.kafkaactiveactive.repair.RepairConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairReport;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairService;

/**
 * Explicit operator-controlled transition. The stop probe must verify closed consumer instances or
 * terminated consumer workers/deployments, not merely missing Kafka group membership. All instances must follow the same active
 * Kafka configuration; this class does not elect or coordinate a global active group.
 */
public final class GroupTransition {
    private final ConsumerSettings settings;
    private final Map<String, Object> sourceProperties;
    private final Map<String, Object> targetProperties;
    private final BooleanSupplier sourceProcessesStopped;

    public GroupTransition(ConsumerSettings settings, Map<String, Object> sourceProperties,
                           Map<String, Object> targetProperties, BooleanSupplier sourceProcessesStopped) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.sourceProperties = Map.copyOf(sourceProperties);
        this.targetProperties = Map.copyOf(targetProperties);
        this.sourceProcessesStopped = Objects.requireNonNull(sourceProcessesStopped, "sourceProcessesStopped");
    }

    /** No target consumer or input-offset reset is performed unless every recovery check succeeds. */
    public Prepared prepare() {
        requireProcessStop();
        try (var source = new CompletionLedger(settings, sourceProperties);
             var target = new CompletionLedger(settings, targetProperties)) {
            requireStoppedGroups(source, target);
        }
        RepairReport data;
        var defaults = RepairConfig.defaults(settings.topic());
        var repairSettings = new RepairConfig(defaults.topic(), defaults.runTimeout(), defaults.interval(),
                defaults.maxIndexEntries(), defaults.maxIndexBytes(), settings.maxEventBytes());
        try (var repair = new RepairService(repairSettings, sourceProperties, targetProperties)) {
            data = repair.runOnce();
        }
        var completions = new CompletionReplicator(settings, sourceProperties, targetProperties).runOnce();
        try (var source = new CompletionLedger(settings, sourceProperties);
             var target = new CompletionLedger(settings, targetProperties)) {
            requireStoppedGroups(source, target);
            var sourceBoundary = source.snapshot();
            var targetBoundary = target.snapshot();
            if (!data.clusterA().clusterId().equals(sourceBoundary.clusterId())
                    || !data.clusterB().clusterId().equals(targetBoundary.clusterId())
                    || !data.clusterA().topicId().equals(source.businessTopicId())
                    || !data.clusterB().topicId().equals(target.businessTopicId())
                    || !completions.topicIdA().equals(sourceBoundary.topicId())
                    || !completions.topicIdB().equals(targetBoundary.topicId())) {
                throw new IllegalStateException("log identities changed during transition preparation");
            }
            return new Prepared(data, completions, sourceBoundary, targetBoundary);
        }
    }

    private void requireProcessStop() {
        if (!sourceProcessesStopped.getAsBoolean()) {
            throw new IllegalStateException("source consumer process/deployment stop is not confirmed");
        }
    }
    private void requireStoppedGroups(CompletionLedger source, CompletionLedger target) {
        requireProcessStop();
        source.requireGroupStopped();
        target.requireGroupStopped();
    }

    public final class Prepared {
        private final RepairReport data;
        private final CompletionReplicator.Report completions;
        private final CompletionLedger.Snapshot sourceBoundary;
        private final CompletionLedger.Snapshot targetBoundary;
        private boolean used;
        private Prepared(RepairReport data, CompletionReplicator.Report completions,
                         CompletionLedger.Snapshot sourceBoundary, CompletionLedger.Snapshot targetBoundary) {
            this.data = data;
            this.completions = completions;
            this.sourceBoundary = sourceBoundary;
            this.targetBoundary = targetBoundary;
        }
        public RepairReport dataReport() { return data; }
        public CompletionReplicator.Report completionReport() { return completions; }

        /** One use only. Run on the thread that will subsequently own the target SDK consumer. */
        public synchronized ActiveKafkaConsumer startTarget() {
            if (used) { throw new IllegalStateException("transition preparation was already used"); }
            used = true;
            requireProcessStop();
            try (var source = new CompletionLedger(settings, sourceProperties);
                 var target = new CompletionLedger(settings, targetProperties)) {
                requireStoppedGroups(source, target);
                source.verify(sourceBoundary);
                target.verify(targetBoundary);
                source.verifyBusinessBoundary(data.clusterA());
                target.verifyBusinessBoundary(data.clusterB());
                if (!source.snapshot().end().equals(sourceBoundary.end())
                        || !target.snapshot().end().equals(targetBoundary.end())
                        || !source.businessTopicId().equals(data.clusterA().topicId())
                        || !target.businessTopicId().equals(data.clusterB().topicId())) {
                    throw new IllegalStateException("prepared completion/data boundary changed; prepare again");
                }
                target.resetInputOffsets(targetBoundary);
            }
            requireProcessStop();
            return new ActiveKafkaConsumer(settings, targetProperties, sourceProperties);
        }
    }
}

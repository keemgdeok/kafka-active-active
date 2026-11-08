package io.github.keemgdeok.kafkaactiveactive.consume;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;

/**
 * Thread-confined consumer for one active Kafka. commit performs bounded blocking native I/O on the
 * owning thread and returns a completed stage. UNKNOWN is terminal: close and reopen to replay history.
 * External application side effects are not part of the Kafka transaction.
 */
public final class ActiveKafkaConsumer implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(ActiveKafkaConsumer.class.getName());
    private final Thread owner = Thread.currentThread();
    private final ConsumerSettings settings;
    private final Map<String, Object> peerProperties;
    private final KafkaConsumer<byte[], byte[]> input;
    private final CompletionLedger ledger;
    private final String inputTopicId;
    private final java.util.function.BiConsumer<Integer, CommitPoint> commitHook;
    private CompletionLedger peer;
    private final Map<Integer, KafkaProducer<byte[], byte[]>> transactions = new HashMap<>();
    private CompletedIndex completed;
    private CompletionLedger.Snapshot ledgerBoundary;
    private DeliveryWindow outstanding;
    private Batch empty = new Batch(List.of());
    private boolean closed;
    private String fatal;
    private long filtered;
    private long delivered;

    public ActiveKafkaConsumer(ConsumerSettings settings, Map<String, Object> activeProperties,
                               Map<String, Object> peerProperties) {
        this(settings, activeProperties, peerProperties, (partition, point) -> {});
    }

    ActiveKafkaConsumer(ConsumerSettings settings, Map<String, Object> activeProperties,
                        Map<String, Object> peerProperties,
                        java.util.function.BiConsumer<Integer, CommitPoint> commitHook) {
        this.settings = settings;
        this.commitHook = commitHook;
        this.peerProperties = Map.copyOf(peerProperties);
        if (!activeProperties.containsKey("bootstrap.servers") || !peerProperties.containsKey("bootstrap.servers")
                || activeProperties.get("bootstrap.servers").equals(peerProperties.get("bootstrap.servers"))) {
            throw new IllegalArgumentException("active and peer configurations must be independent");
        }
        ledger = new CompletionLedger(settings, activeProperties);
        KafkaConsumer<byte[], byte[]> reader = null;
        try {
            inputTopicId = ledger.businessTopicId();
            var config = CompletionLedger.readerProperties(settings, activeProperties);
            config.put("group.id", settings.groupId());
            config.put("auto.offset.reset", "none");
            config.put("group.protocol", "classic");
            config.put("partition.assignment.strategy", RangeAssignor.class.getName());
            config.put("session.timeout.ms", 6_000);
            config.put("heartbeat.interval.ms", 500);
            config.put("max.poll.interval.ms", 120_000);
            reader = new KafkaConsumer<>(config);
            input = reader;
            completed = new CompletedIndex(settings);
            input.subscribe(List.of(settings.topic()), new Assignments());
        } catch (Exception failure) {
            if (reader != null) { reader.close(CloseOptions.timeout(Duration.ZERO)); }
            ledger.close();
            if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            throw new IllegalStateException("cannot initialize active Kafka consumer", failure);
        }
    }

    public Batch poll(Duration timeout) {
        requireUsable();
        if (timeout.isNegative() || timeout.compareTo(settings.operationTimeout()) > 0) {
            throw new IllegalArgumentException("poll timeout must be 0..operationTimeout");
        }
        if (outstanding != null) { throw new IllegalStateException("commit or close the outstanding batch before polling"); }
        try {
            verifyInputIdentity();
            var records = new ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<byte[], byte[]>>();
            input.poll(timeout).forEach(records::add);
            verifyInputIdentity();
            if (records.isEmpty()) { return empty = new Batch(List.of()); }
            Map<Integer, Long> ends = new HashMap<>();
            for (TopicPartition partition : input.assignment()) {
                ends.put(partition.partition(), input.position(partition, settings.operationTimeout()));
            }
            outstanding = new DeliveryWindow(records, ends, completed, settings.maxEventBytes());
            filtered += outstanding.filtered();
            if (outstanding.batch().isEmpty()) {
                commit(outstanding.batch().processed()).toCompletableFuture().join();
                requireUsable();
                return empty = new Batch(List.of());
            }
            delivered += outstanding.batch().records().size();
            return outstanding.batch();
        } catch (RuntimeException failure) {
            fatal = failure.getClass().getSimpleName();
            throw failure;
        }
    }

    public CompletionStage<CommitResult> commit(ProcessedBatch batch) {
        requireUsable();
        if (outstanding == null) {
            if (empty.owns(batch) && batch.count() == 0) {
                return CompletableFuture.completedStage(new CommitResult(CommitResult.Status.CONFIRMED, Set.of(), Set.of()));
            }
            throw new IllegalArgumentException("no matching outstanding batch");
        }
        DeliveryWindow.Plan plan = outstanding.plan(batch);
        Set<String> local = new HashSet<>();
        Set<String> mirrored = new HashSet<>();
        List<CompletionMarker> committedMarkers = new ArrayList<>();
        try {
            verifyInputIdentity();
            ledger.verify(ledgerBoundary);
            for (var entry : plan.next().entrySet()) {
                int partition = entry.getKey();
                TopicPartition topicPartition = new TopicPartition(settings.topic(), partition);
                var producer = transactions.get(partition);
                if (producer == null || !input.assignment().contains(topicPartition)) {
                    throw new IllegalStateException("input partition no longer assigned");
                }
                var markers = plan.markers().getOrDefault(partition, List.of());
                producer.beginTransaction();
                for (var marker : markers) { producer.send(marker.record(settings.completionTopic())); }
                commitHook.accept(partition, CommitPoint.MARKERS_SENT);
                producer.sendOffsetsToTransaction(Map.of(topicPartition,
                        new OffsetAndMetadata(entry.getValue(), offsetMetadata())), input.groupMetadata());
                commitHook.accept(partition, CommitPoint.OFFSETS_STAGED);
                producer.commitTransaction();
                commitHook.accept(partition, CommitPoint.LOCAL_COMMITTED);
                for (var marker : markers) {
                    completed.add(marker);
                    local.add(marker.id());
                    committedMarkers.add(marker);
                }
            }
            for (var entry : plan.rewind().entrySet()) {
                input.seek(new TopicPartition(settings.topic(), entry.getKey()), entry.getValue());
            }
            outstanding = null;
        } catch (RuntimeException failure) {
            fatal = failure.getClass().getSimpleName();
            closeTransactions();
            LOG.log(System.Logger.Level.WARNING, "completion commit UNKNOWN for group {0}; confirmed IDs={1}; failure={2}",
                    settings.groupId(), local.size(), failure.toString());
            return CompletableFuture.completedStage(new CommitResult(CommitResult.Status.UNKNOWN, local, mirrored));
        }
        try {
            if (!committedMarkers.isEmpty()) {
                if (peer == null) { peer = new CompletionLedger(settings, peerProperties); }
                var snapshot = peer.snapshot();
                if (snapshot.clusterId().equals(ledgerBoundary.clusterId())) {
                    throw new IllegalStateException("peer aliases the active Kafka cluster");
                }
                for (var marker : committedMarkers) {
                    peer.append(marker);
                    mirrored.add(marker.id());
                }
                peer.verify(snapshot);
            }
        } catch (RuntimeException unavailable) {
            LOG.log(System.Logger.Level.INFO, "completion peer unavailable for group {0}; local IDs={1}; failure={2}",
                    settings.groupId(), local.size(), unavailable.toString());
            if (peer != null) {
                try { peer.close(); } catch (RuntimeException ignored) { /* Local ACKs remain valid. */ }
                peer = null;
            }
            return CompletableFuture.completedStage(new CommitResult(CommitResult.Status.LOCAL_ONLY, local, mirrored));
        }
        return CompletableFuture.completedStage(new CommitResult(CommitResult.Status.CONFIRMED, local, mirrored));
    }

    public RuntimeStatus status() {
        requireOwner();
        Set<Integer> assignment = new HashSet<>(transactions.keySet());
        return new RuntimeStatus(closed, fatal, Set.copyOf(assignment), completed.size(), delivered, filtered);
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner) { throw new IllegalStateException("consumer must stay on its owning thread"); }
    }
    private void verifyInputIdentity() {
        if (!inputTopicId.equals(ledger.businessTopicId())) {
            throw new IllegalStateException("business topic identity changed; consumer recovery is required");
        }
    }
    private String offsetMetadata() { return "kaa-ledger=" + ledgerBoundary.topicId() + ";input=" + inputTopicId; }
    private void requireUsable() {
        requireOwner();
        if (closed || fatal != null) { throw new IllegalStateException("consumer is closed or requires recovery: " + fatal); }
    }
    private void closeTransactions() {
        transactions.values().forEach(producer -> {
            try { producer.close(Duration.ZERO); } catch (RuntimeException ignored) { /* Continue releasing other partitions. */ }
        });
        transactions.clear();
    }
    @Override public void close() {
        requireOwner();
        if (closed) { return; }
        closed = true;
        try { input.close(CloseOptions.timeout(settings.operationTimeout())); }
        finally {
            closeTransactions();
            try { ledger.close(); }
            finally { if (peer != null) { peer.close(); } }
        }
    }

    public record RuntimeStatus(boolean closed, String failure, Set<Integer> assignedPartitions,
                                int completedIds, long deliveredEvents, long filteredRecords) {}

    enum CommitPoint { MARKERS_SENT, OFFSETS_STAGED, LOCAL_COMMITTED }

    private final class Assignments implements ConsumerRebalanceListener {
        @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            if (outstanding != null) {
                outstanding.beginnings().forEach((partition, offset) -> {
                    TopicPartition topicPartition = new TopicPartition(settings.topic(), partition);
                    if (!partitions.contains(topicPartition) && input.assignment().contains(topicPartition)) {
                        input.seek(topicPartition, offset);
                    }
                });
                outstanding = null;
            }
            for (TopicPartition partition : partitions) {
                var producer = transactions.remove(partition.partition());
                if (producer != null) { producer.close(Duration.ZERO); }
            }
        }
        @Override public void onPartitionsLost(Collection<TopicPartition> partitions) { onPartitionsRevoked(partitions); }
        @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                transactions.computeIfAbsent(partition.partition(), ledger::transactionProducer);
            }
            ledgerBoundary = ledger.snapshot();
            completed = ledger.replay(ledgerBoundary, partitions.stream().map(TopicPartition::partition)
                    .collect(java.util.stream.Collectors.toSet()));
            LOG.log(System.Logger.Level.INFO, "completion replay group={0}, partitions={1}, IDs={2}, ledger={3}",
                    settings.groupId(), partitions.toString(), completed.size(), ledgerBoundary.topicId());
            var offsets = input.committed(Set.copyOf(partitions), settings.operationTimeout());
            offsets.values().stream().filter(java.util.Objects::nonNull).forEach(offset -> {
                if (!offset.metadata().equals(offsetMetadata())) {
                    throw new IllegalStateException("input offset belongs to missing/different completion history");
                }
            });
            if (input.beginningOffsets(partitions, settings.operationTimeout()).values().stream().anyMatch(offset -> offset != 0)) {
                throw new IllegalStateException("business retained beginning was lost");
            }
            for (TopicPartition partition : partitions) {
                var offset = offsets.get(partition);
                input.seek(partition, offset == null ? 0 : offset.offset());
            }
        }
    }
}

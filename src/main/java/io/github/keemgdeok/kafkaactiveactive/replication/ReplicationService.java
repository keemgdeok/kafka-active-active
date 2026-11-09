package io.github.keemgdeok.kafkaactiveactive.replication;

import static io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess.await;
import static io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess.deadline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess;

/**
 * Explicit SDK lifecycle; one service owner per topic/pair. RAWs are copied once per committed
 * source position. A target append and source progress are not a distributed transaction:
 * a crash between them can duplicate physical records.
 */
public final class ReplicationService implements AutoCloseable {
    public enum State { STOPPED, CONNECTING, RUNNING, RETRYING, FAILED, CLOSED }
    public record DirectionStatus(State state, LogIdentity source, LogIdentity target,
                                  Map<Integer, Long> committedOffsets, long replicatedRecords, String failure) {
        public DirectionStatus { committedOffsets = Map.copyOf(committedOffsets); }
    }
    public record Status(DirectionStatus aToB, DirectionStatus bToA, boolean closed) {}
    enum Stage { BATCH_READ, BEFORE_TARGET_APPEND, AFTER_TARGET_ACK, BEFORE_SOURCE_COMMIT, AFTER_SOURCE_COMMIT }
    record FaultPoint(Stage stage, int sourceSide, int partition, long offset, int batchSize) {}
    private static final System.Logger LOG = System.getLogger(ReplicationService.class.getName());
    private final ReplicationConfig config;
    private final Consumer<FaultPoint> hook;
    private final LongSupplier budgetClock;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Direction aToB;
    private final Direction bToA;
    private boolean started;

    public ReplicationService(ReplicationConfig config, Map<String, Object> a, Map<String, Object> b) {
        this(config, a, b, ignored -> {});
    }
    ReplicationService(ReplicationConfig config, Map<String, Object> a, Map<String, Object> b,
                       Consumer<FaultPoint> hook) {
        this(config, a, b, hook, System::nanoTime);
    }
    ReplicationService(ReplicationConfig config, Map<String, Object> a, Map<String, Object> b,
                       Consumer<FaultPoint> hook, LongSupplier budgetClock) {
        this.budgetClock = java.util.Objects.requireNonNull(budgetClock, "budgetClock");
        this.config = java.util.Objects.requireNonNull(config, "config");
        this.hook = java.util.Objects.requireNonNull(hook, "hook");
        for (var map : List.of(a, b)) {
            if (!map.containsKey("bootstrap.servers")) { throw new IllegalArgumentException("bootstrap.servers required"); }
            KafkaAccess.writer(map, config.operationTimeout(), config.maxEventBytes());
        }
        if (java.util.Objects.equals(a.get("bootstrap.servers"), b.get("bootstrap.servers"))) {
            throw new IllegalArgumentException("independent bootstrap configurations required");
        }
        aToB = new Direction(0, Map.copyOf(a), Map.copyOf(b));
        bToA = new Direction(1, Map.copyOf(b), Map.copyOf(a));
    }
    public synchronized void start() {
        if (closed.get()) { throw new IllegalStateException("replication service closed"); }
        if (!started) { started = true; aToB.thread.start(); bToA.thread.start(); }
    }
    public Status status() {
        return new Status(aToB.status.get(), bToA.status.get(),
                closed.get() && !aToB.thread.isAlive() && !bToA.thread.isAlive());
    }
    @Override public void close() {
        long end = deadline(config.closeTimeout());
        synchronized (this) {
            if (closed.compareAndSet(false, true)) {
                for (var direction : List.of(aToB, bToA)) {
                    var consumer = direction.reader;
                    if (consumer != null) { consumer.wakeup(); }
                    direction.thread.interrupt();
                }
            }
        }
        for (var direction : List.of(aToB, bToA)) {
            try {
                long remaining = end - System.nanoTime();
                if (direction.thread.isAlive() && remaining > 0) {
                    TimeUnit.NANOSECONDS.timedJoin(direction.thread, remaining);
                }
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); break; }
        }
        if (aToB.thread.isAlive() || bToA.thread.isAlive()) {
            throw new IllegalStateException("replication workers did not stop within closeTimeout");
        }
    }

    private final class Direction {
        private final int side;
        private final Map<String, Object> sourceProperties;
        private final Map<String, Object> targetProperties;
        private final AtomicReference<DirectionStatus> status = new AtomicReference<>(
                new DirectionStatus(State.STOPPED, null, null, Map.of(), 0, null));
        private final Thread thread;
        private volatile KafkaConsumer<byte[], byte[]> reader;
        private long copied;

        Direction(int side, Map<String, Object> source, Map<String, Object> target) {
            this.side = side; sourceProperties = source; targetProperties = target;
            thread = Thread.ofPlatform().daemon().name("kaa-replication-" + side).unstarted(this::run);
        }
        private void publish(State state, LogIdentity source, LogIdentity target, Map<Integer, Long> offsets,
                             String failure) {
            status.set(new DirectionStatus(state, source, target, offsets, copied, failure));
        }
        private void run() {
            try {
                while (!closed.get()) {
                    publish(State.CONNECTING, null, null, Map.of(), null);
                    try (var source = new KafkaAccess(sourceProperties, config.operationTimeout());
                         var target = new KafkaAccess(targetProperties, config.operationTimeout())) {
                        long end = deadline(config.operationTimeout());
                        var sourceId = source.identity(config.topic(), end, true);
                        var targetId = target.identity(config.topic(), end, true);
                        config.groupId(sourceId, targetId); // Reject aliases resolving to the same native Kafka.
                        publish(State.CONNECTING, sourceId, targetId, Map.of(), null);
                        source.confirmationTopic(config, end);
                        try (var session = new Session(sourceId, targetId, source, target)) {
                            process(source, target, session);
                        }
                    } catch (GenerationChanged changed) {
                        // New native generation gets a different group/transaction namespace.
                    } catch (RuntimeException failure) {
                        if (closed.get()) { break; }
                        Throwable cause = failure;
                        boolean retryable = false;
                        for (Throwable current = failure; current != null; current = current.getCause()) {
                            cause = current;
                            if (current instanceof IllegalArgumentException || current instanceof IllegalStateException
                                    || current instanceof ProducerFencedException || current instanceof AuthorizationException
                                    || current instanceof AuthenticationException
                                    || current instanceof org.apache.kafka.clients.consumer.InvalidOffsetException) {
                                retryable = false;
                                break;
                            }
                            if (current instanceof RetriableException || current instanceof GenerationChanged
                                    || current instanceof org.apache.kafka.clients.consumer.CommitFailedException) {
                                retryable = true;
                            }
                        }
                        boolean fatal = !retryable;
                        var previous = status.get();
                        publish(fatal ? State.FAILED : State.RETRYING, previous.source(), previous.target(),
                                previous.committedOffsets(), cause.getClass().getSimpleName() + ": " + cause.getMessage());
                        if (fatal) { LOG.log(System.Logger.Level.ERROR, "replication direction failed", failure); return; }
                        try { Thread.sleep(250); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
                    }
                }
            } finally {
                reader = null;
                if (closed.get()) {
                    var previous = status.get();
                    publish(State.CLOSED, previous.source(), previous.target(), previous.committedOffsets(), previous.failure());
                } else if (status.get().state() != State.FAILED) {
                    var previous = status.get();
                    publish(State.FAILED, previous.source(), previous.target(), previous.committedOffsets(),
                            "replication thread stopped unexpectedly");
                }
            }
        }
        private void process(KafkaAccess source, KafkaAccess target, Session session) {
            long nextProbe = 0;
            while (!closed.get()) {
                ConsumerRecords<byte[], byte[]> records;
                try { records = session.consumer.poll(Duration.ofMillis(250)); }
                catch (org.apache.kafka.clients.consumer.InvalidOffsetException invalid) {
                    requireGeneration(source, target, session, deadline(config.operationTimeout()));
                    throw invalid;
                }
                if (records.isEmpty()) {
                    if (System.nanoTime() >= nextProbe) {
                        requireGeneration(source, target, session, deadline(config.operationTimeout()));
                        nextProbe = System.nanoTime() + Duration.ofSeconds(1).toNanos();
                    }
                    continue;
                }
                requireGeneration(source, target, session, deadline(config.operationTimeout()));
                var first = records.iterator().next();
                hook.accept(new FaultPoint(Stage.BATCH_READ, side, first.partition(), first.offset(), records.count()));
                var acknowledgements = new ArrayList<ReplicationAck>();
                var progress = new HashMap<TopicPartition, OffsetAndMetadata>();
                var rewind = new HashMap<TopicPartition, OffsetAndMetadata>();
                records.forEach(r -> rewind.putIfAbsent(new TopicPartition(r.topic(), r.partition()),
                        new OffsetAndMetadata(r.offset(), r.leaderEpoch(), "")));
                long budgetEnd = budgetClock.getAsLong() + config.operationTimeout().toNanos();
                int handled = 0;
                long rawBytes = 0;
                long byteBudget = Math.max(1_048_576L, (long) config.maxEventBytes() + 1_024);
                boolean unavailable = false;
                ConsumerRecord<byte[], byte[]> last = null;
                for (var record : records) {
                    // Commit an ACKed prefix rather than replaying an entire slow batch forever.
                    if (handled > 0 && budgetClock.getAsLong() >= budgetEnd) { break; }
                    var identity = EventCodec.identity(record, config.maxEventBytes());
                    if (EventCodec.kind(record) == EventCodec.Kind.RAW) {
                        long bytes = (long) record.key().length + record.value().length
                                + identity.id().getBytes(java.nio.charset.StandardCharsets.UTF_8).length + 1_024;
                        if (handled > 0 && rawBytes + bytes > byteBudget) { break; }
                        try {
                            point(Stage.BEFORE_TARGET_APPEND, record);
                            long appendDeadline = deadline(config.operationTimeout());
                            var metadata = await(session.targetProducer.send(
                                    EventCodec.replica(config.topic(), record, config.maxEventBytes())),
                                    appendDeadline);
                            if (!metadata.hasOffset() || metadata.partition() != record.partition()) {
                                throw new IllegalStateException("target did not ACK the expected partition");
                            }
                            point(Stage.AFTER_TARGET_ACK, record);
                            acknowledgements.add(new ReplicationAck(EventCodec.attempt(record), identity.id(), identity.hash(),
                                    record.partition(), session.sourceId, record.offset(), session.targetId, metadata.offset()));
                            rawBytes += bytes;
                        } catch (RetriableException retry) { unavailable = true; break; }
                    }
                    progress.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, record.leaderEpoch(), ""));
                    handled++;
                    last = record;
                }
                if (handled > 0) {
                    for (var partition : records.partitions()) {
                        var next = progress.get(partition);
                        var batch = records.records(partition);
                        if (next != null && next.offset() == batch.getLast().offset() + 1) {
                            progress.put(partition, records.nextOffsets().get(partition));
                        }
                    }
                    long end = deadline(config.operationTimeout());
                    requireGeneration(source, target, session, end);
                    session.sourceTransaction.beginTransaction();
                    var receipts = new ArrayList<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>>();
                    for (var ack : acknowledgements) {
                        receipts.add(session.sourceTransaction.send(ack.record(config.confirmationTopic())));
                    }
                    for (var receipt : receipts) { await(receipt, end); }
                    point(Stage.BEFORE_SOURCE_COMMIT, last);
                    session.sourceTransaction.sendOffsetsToTransaction(progress, session.consumer.groupMetadata());
                    session.sourceTransaction.commitTransaction();
                    point(Stage.AFTER_SOURCE_COMMIT, last);
                    copied += acknowledgements.size();
                    var offsets = new HashMap<Integer, Long>(status.get().committedOffsets());
                    progress.forEach((tp, offset) -> offsets.put(tp.partition(), offset.offset()));
                    publish(State.RUNNING, session.sourceId, session.targetId, offsets, null);
                }
                if (handled < records.count()) {
                    progress.forEach(rewind::put);
                    session.pausedForRetry = true;
                    session.consumer.pause(session.consumer.assignment());
                    if (unavailable) {
                        publish(State.RETRYING, session.sourceId, session.targetId,
                                status.get().committedOffsets(), "target ACK unavailable");
                    }
                    session.consumer.poll(unavailable ? Duration.ofMillis(250) : Duration.ZERO).forEach(r -> {
                        var tp = new TopicPartition(r.topic(), r.partition());
                        var next = new OffsetAndMetadata(r.offset(), r.leaderEpoch(), "");
                        rewind.merge(tp, next, (left, right) -> left.offset() <= right.offset() ? left : right);
                    });
                    for (var entry : rewind.entrySet()) {
                        if (session.consumer.assignment().contains(entry.getKey())) {
                            session.consumer.seek(entry.getKey(), entry.getValue());
                        }
                    }
                    session.pausedForRetry = false;
                    session.consumer.resume(session.consumer.assignment());
                }
            }
        }
        private void requireGeneration(KafkaAccess source, KafkaAccess target, Session session, long end) {
            if (!session.sourceId.equals(source.identity(config.topic(), end, true))
                    || !session.targetId.equals(target.identity(config.topic(), end, true))) {
                throw new GenerationChanged();
            }
        }
        private void point(Stage stage, ConsumerRecord<byte[], byte[]> record) {
            hook.accept(new FaultPoint(stage, side, record.partition(), record.offset(), 1));
        }

        private final class Session implements AutoCloseable {
            private final LogIdentity sourceId;
            private final LogIdentity targetId;
            private final KafkaConsumer<byte[], byte[]> consumer;
            private KafkaProducer<byte[], byte[]> targetProducer;
            private KafkaProducer<byte[], byte[]> sourceTransaction;
            private boolean pausedForRetry;
            Session(LogIdentity source, LogIdentity target, KafkaAccess sourceAccess, KafkaAccess targetAccess) {
                sourceId = source; targetId = target;
                var props = KafkaAccess.reader(sourceProperties, config.operationTimeout(),
                        config.maxBatchRecords(), config.maxEventBytes());
                props.put("group.id", config.groupId(source, target));
                consumer = new KafkaConsumer<>(props);
                reader = consumer;
                try {
                    var writer = KafkaAccess.writer(sourceProperties, config.operationTimeout(), config.maxEventBytes());
                    writer.put("transactional.id", config.transactionalId(source, target));
                    writer.put("transaction.timeout.ms", (int) Math.max(30_000, 4 * config.operationTimeout().toMillis()));
                    sourceTransaction = new KafkaProducer<>(writer);
                    sourceTransaction.initTransactions();
                    targetProducer = new KafkaProducer<>(KafkaAccess.writer(targetProperties,
                            config.operationTimeout(), config.maxEventBytes()));
                    consumer.subscribe(List.of(config.topic()), new ConsumerRebalanceListener() {
                        @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}
                        @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            requireGeneration(sourceAccess, targetAccess, Session.this, deadline(config.operationTimeout()));
                            var offsets = consumer.committed(java.util.Set.copyOf(partitions), config.operationTimeout());
                            var beginnings = consumer.beginningOffsets(partitions, config.operationTimeout());
                            var ends = consumer.endOffsets(partitions, config.operationTimeout());
                            var progress = new HashMap<Integer, Long>();
                            for (var partition : partitions) {
                                OffsetAndMetadata offset = offsets.get(partition);
                                long next = offset == null ? 0 : offset.offset();
                                if (beginnings.get(partition) != 0 || next < 0 || next > ends.get(partition)) {
                                    throw new IllegalStateException("replication cursor outside retained log");
                                }
                                if (offset != null && next > 0 && offset.leaderEpoch().isEmpty()) {
                                    throw new IllegalStateException("replication cursor is missing leader epoch");
                                }
                                if (offset == null) { consumer.seek(partition, 0); }
                                else { consumer.seek(partition, offset); }
                                progress.put(partition.partition(), next);
                            }
                            publish(State.RUNNING, sourceId, targetId, progress, null);
                            if (pausedForRetry) { consumer.pause(partitions); }
                        }
                    });
                } catch (RuntimeException failure) { close(); throw failure; }
            }
            @Override public void close() {
                boolean interrupted = Thread.interrupted();
                try { consumer.close(org.apache.kafka.clients.consumer.CloseOptions.timeout(Duration.ofSeconds(1))); }
                catch (WakeupException | org.apache.kafka.common.errors.InterruptException ignored) { /* closing */ }
                finally {
                    try { if (targetProducer != null) { targetProducer.close(Duration.ZERO); } }
                    finally {
                        try { if (sourceTransaction != null) { sourceTransaction.close(Duration.ZERO); } }
                        finally {
                            reader = null;
                            if (interrupted) { Thread.currentThread().interrupt(); }
                        }
                    }
                }
            }
        }
    }
    private static final class GenerationChanged extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}

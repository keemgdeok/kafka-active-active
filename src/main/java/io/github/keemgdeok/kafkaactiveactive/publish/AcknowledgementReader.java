package io.github.keemgdeok.kafkaactiveactive.publish;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess;
import io.github.keemgdeok.kafkaactiveactive.replication.LogIdentity;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;

/** A bounded streaming reader, not an index of receipt history. Only live UUIDs are decoded. */
final class AcknowledgementReader {
    record Generation(LogIdentity source, LogIdentity target) {}
    private final RoutedKafkaPublisher owner;
    private final int side;
    private final Map<String, Object> source;
    private final Map<String, Object> target;
    private final ReplicationConfig config;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private volatile KafkaConsumer<byte[], byte[]> consumer;
    private volatile String failure;

    AcknowledgementReader(RoutedKafkaPublisher owner, int side, Map<String, Object> source,
                          Map<String, Object> target, ReplicationConfig config) {
        this.owner = owner; this.side = side; this.source = Map.copyOf(source);
        this.target = Map.copyOf(target); this.config = config;
        thread = Thread.ofPlatform().daemon().name("kaa-confirmation-" + side).unstarted(this::run);
    }
    void start() { thread.start(); }
    String failure() { return failure; }
    void stop() {
        closed.set(true);
        var reader = consumer;
        if (reader != null) { reader.wakeup(); }
        thread.interrupt();
    }
    void join(long end) throws InterruptedException {
        long remaining = end - System.nanoTime();
        if (thread.isAlive() && remaining > 0) { java.util.concurrent.TimeUnit.NANOSECONDS.timedJoin(thread, remaining); }
    }
    private void run() {
        while (!closed.get()) {
            try (var from = new KafkaAccess(source, config.operationTimeout());
                 var to = new KafkaAccess(target, config.operationTimeout())) {
                var reader = new KafkaConsumer<byte[], byte[]>(KafkaAccess.reader(source,
                        config.operationTimeout(), 100, config.maxEventBytes()));
                consumer = reader;
                var generation = generation(from, to);
                var ackId = from.identity(config.confirmationTopic(), KafkaAccess.deadline(config.operationTimeout()), false);
                var partitions = List.of(new TopicPartition(config.confirmationTopic(), 0),
                        new TopicPartition(config.confirmationTopic(), 1));
                reader.assign(partitions);
                // Also on reconnect: never seek past receipts for RAWs sent while this reader was absent.
                // Retention bounds the log; memory holds only one poll plus live publisher attempts.
                var starts = reader.beginningOffsets(partitions, config.operationTimeout());
                starts.forEach(reader::seek);
                owner.observe(side, generation);
                failure = null;
                long probe = System.nanoTime();
                while (!closed.get()) {
                    var records = reader.poll(Duration.ofMillis(100));
                    if (!records.isEmpty() || System.nanoTime() >= probe) {
                        var current = generation(from, to);
                        var currentAck = from.identity(config.confirmationTopic(),
                                KafkaAccess.deadline(config.operationTimeout()), false);
                        if (!generation.equals(current) || !ackId.equals(currentAck)) { break; }
                        probe = System.nanoTime() + Duration.ofSeconds(1).toNanos();
                    }
                    for (var record : records) { owner.receipt(side, record, generation); }
                }
            } catch (RuntimeException unavailable) {
                if (!closed.get()) { failure = unavailable.getClass().getSimpleName() + ": " + unavailable.getMessage(); }
            } finally {
                var reader = consumer; consumer = null; owner.observe(side, null);
                if (reader != null) {
                    boolean interrupted = Thread.interrupted();
                    try { reader.close(org.apache.kafka.clients.consumer.CloseOptions.timeout(Duration.ZERO)); }
                    finally { if (interrupted) { Thread.currentThread().interrupt(); } }
                }
            }
            if (!closed.get()) {
                try { Thread.sleep(250); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); return; }
            }
        }
    }
    private Generation generation(KafkaAccess sourceAccess, KafkaAccess targetAccess) {
        long end = KafkaAccess.deadline(config.operationTimeout());
        var sourceId = sourceAccess.identity(config.topic(), end, true);
        var targetId = targetAccess.identity(config.topic(), end, true);
        config.groupId(sourceId, targetId); // Native cluster aliases cannot form an independent pair.
        return new Generation(sourceId, targetId);
    }
}

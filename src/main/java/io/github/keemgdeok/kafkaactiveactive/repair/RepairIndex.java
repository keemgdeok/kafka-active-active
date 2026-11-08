package io.github.keemgdeok.kafkaactiveactive.repair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

final class RepairIndex {
    private final RepairConfig config;
    private final Map<String, Entry> entries = new HashMap<>();
    private long bytes;
    private long records;

    RepairIndex(RepairConfig config) {
        this.config = config;
    }

    void add(ConsumerRecord<byte[], byte[]> record, int side) {
        Value value = decode(record, config.maxEventBytes());
        Entry entry = entries.get(value.id());
        if (entry == null) {
            long cost = 192L + 2L * value.id().length();
            if (entries.size() >= config.maxIndexEntries() || cost > config.maxIndexBytes() - bytes) {
                throw new RepairException(RepairException.Reason.INDEX_LIMIT, "repair index limit exceeded");
            }
            entry = new Entry(value.hash(), record.partition());
            entries.put(value.id(), entry);
            bytes += cost;
        } else {
            requireMatch(entry, value, record.partition());
        }
        entry.present[side] = true;
        records++;
    }

    boolean missingOnOtherSide(ConsumerRecord<byte[], byte[]> record, int source) {
        Value value = decode(record, config.maxEventBytes());
        Entry entry = entries.get(value.id());
        if (entry == null) {
            throw new RepairException(RepairException.Reason.LOG_CHANGED, "record was not in the captured index");
        }
        requireMatch(entry, value, record.partition());
        return entry.present[source] && !entry.present[1 - source];
    }

    void copied(ConsumerRecord<byte[], byte[]> record, int target) {
        Value value = decode(record, config.maxEventBytes());
        Entry entry = entries.get(value.id());
        requireMatch(entry, value, record.partition());
        entry.present[target] = true;
    }

    int size() { return entries.size(); }
    long bytes() { return bytes; }
    long records() { return records; }

    void requireFullyCopied() {
        if (entries.values().stream().anyMatch(entry -> !entry.present[0] || !entry.present[1])) {
            throw new RepairException(RepairException.Reason.LOG_CHANGED, "a captured event was not copied");
        }
    }

    private static void requireMatch(Entry entry, Value value, int partition) {
        if (entry.partition != partition || !Arrays.equals(entry.hash, value.hash())) {
            throw new RepairException(RepairException.Reason.CONFLICT,
                    "same event ID has different key, payload or partition");
        }
    }

    static Value decode(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        try {
            var identity = io.github.keemgdeok.kafkaactiveactive.internal.EventCodec.identity(record, maxBytes);
            return new Value(identity.id(), identity.hash());
        } catch (IllegalArgumentException invalid) {
            throw new RepairException(RepairException.Reason.INVALID_RECORD, invalid.getMessage(), invalid);
        }
    }

    record Value(String id, byte[] hash) {}
    private static final class Entry {
        private final byte[] hash;
        private final int partition;
        private final boolean[] present = new boolean[2];
        private Entry(byte[] hash, int partition) {
            this.hash = hash;
            this.partition = partition;
        }
    }
}

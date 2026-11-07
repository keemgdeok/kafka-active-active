package io.github.keemgdeok.kafkaactiveactive.repair;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import io.github.keemgdeok.kafkaactiveactive.publish.DualKafkaPublisher;

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
        byte[] id = uniqueHeader(record, DualKafkaPublisher.EVENT_ID_HEADER);
        byte[] version = uniqueHeader(record, DualKafkaPublisher.FORMAT_HEADER);
        if (!Arrays.equals(version, new byte[] {1}) || record.key() == null || record.value() == null
                || (long) id.length + record.key().length + record.value().length > maxBytes) {
            throw invalid("unsupported format, null record bytes, or oversized event");
        }
        String eventId = text(id);
        if (eventId.isBlank() || text(record.key()).isBlank()
                || record.partition() != Utils.toPositive(Utils.murmur2(record.key())) % 2) {
            throw invalid("invalid event ID, key or partition");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(4).putInt(record.key().length).array());
            digest.update(record.key());
            digest.update(record.value());
            return new Value(eventId, digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static byte[] uniqueHeader(ConsumerRecord<byte[], byte[]> record, String key) {
        byte[] value = null;
        int count = 0;
        for (Header header : record.headers().headers(key)) {
            count++;
            value = header.value();
        }
        if (count != 1 || value == null) {
            throw invalid("missing or repeated SDK header: " + key);
        }
        return value;
    }

    private static String text(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException failure) {
            throw invalid("SDK identity and key must be valid UTF-8");
        }
    }

    private static RepairException invalid(String message) {
        return new RepairException(RepairException.Reason.INVALID_RECORD, message);
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

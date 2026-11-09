package io.github.keemgdeok.kafkaactiveactive.internal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import io.github.keemgdeok.kafkaactiveactive.publish.Event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Utils;

/** Shared SDK wire format. This is an internal implementation API. */
public final class EventCodec {
    public static final String EVENT_ID_HEADER = "kaa.event.id";
    public static final String FORMAT_HEADER = "kaa.format";
    public static final String KIND_HEADER = "kaa.kind";
    public static final String ATTEMPT_HEADER = "kaa.attempt";
    public enum Kind { LEGACY, RAW, REPLICA }
    private EventCodec() {}

    public static ProducerRecord<byte[], byte[]> encode(String topic, Event event) {
        return new ProducerRecord<>(topic, event.partition(), null,
                event.key().getBytes(StandardCharsets.UTF_8), event.payload(), List.of(
                new RecordHeader(EVENT_ID_HEADER, event.eventId().getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(FORMAT_HEADER, new byte[] {1})));
    }

    public static ProducerRecord<byte[], byte[]> raw(String topic, Event event, UUID attempt) {
        return new ProducerRecord<>(topic, event.partition(), null,
                event.key().getBytes(StandardCharsets.UTF_8), event.payload(), List.of(
                new RecordHeader(EVENT_ID_HEADER, event.eventId().getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(FORMAT_HEADER, new byte[] {2}),
                new RecordHeader(KIND_HEADER, new byte[] {0}),
                new RecordHeader(ATTEMPT_HEADER, uuid(attempt))));
    }

    public static ProducerRecord<byte[], byte[]> replica(String topic, ConsumerRecord<byte[], byte[]> record,
                                                        int maxBytes) {
        identity(record, maxBytes);
        var headers = new RecordHeaders(record.headers().toArray());
        headers.remove(FORMAT_HEADER).remove(KIND_HEADER).remove(ATTEMPT_HEADER);
        headers.add(FORMAT_HEADER, new byte[] {2}).add(KIND_HEADER, new byte[] {1});
        return new ProducerRecord<>(topic, record.partition(), record.timestamp() >= 0
                ? record.timestamp() : null, record.key(), record.value(), headers);
    }

    public static Kind kind(ConsumerRecord<byte[], byte[]> record) {
        byte[] version = uniqueHeader(record, FORMAT_HEADER);
        if (Arrays.equals(version, new byte[] {1})) { return Kind.LEGACY; }
        if (!Arrays.equals(version, new byte[] {2})) { throw invalid("unsupported event format"); }
        byte[] kind = uniqueHeader(record, KIND_HEADER);
        if (Arrays.equals(kind, new byte[] {0})) {
            attempt(record);
            return Kind.RAW;
        }
        if (Arrays.equals(kind, new byte[] {1})) { return Kind.REPLICA; }
        throw invalid("unsupported event kind");
    }

    public static UUID attempt(ConsumerRecord<byte[], byte[]> record) {
        byte[] value = uniqueHeader(record, ATTEMPT_HEADER);
        if (value.length != 16) { throw invalid("invalid attempt UUID"); }
        var buffer = ByteBuffer.wrap(value);
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    public static byte[] uuid(UUID value) {
        return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    public static Event event(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        Identity identity = identity(record, maxBytes);
        return new Event(identity.id(), text(record.key()), record.value());
    }

    public static String utf8(byte[] value) { return text(value); }

    public static Identity identity(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        byte[] id = uniqueHeader(record, EVENT_ID_HEADER);
        kind(record);
        if (record.key() == null || record.value() == null
                || (long) id.length + record.key().length + record.value().length > maxBytes) {
            throw invalid("unsupported format, null record bytes, or oversized event");
        }
        String eventId = text(id);
        if (eventId.isBlank() || text(record.key()).isBlank()
                || record.partition() != Utils.toPositive(Utils.murmur2(record.key())) % 2) {
            throw invalid("invalid event ID, key or partition");
        }
        return new Identity(eventId, hash(text(record.key()), record.value()));
    }

    public static byte[] hash(String key, byte[] payload) {
        try {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
            digest.update(bytes);
            digest.update(payload);
            return digest.digest();
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

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }

    public record Identity(String id, byte[] hash) {}
}

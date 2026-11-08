package io.github.keemgdeok.kafkaactiveactive.internal;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

import io.github.keemgdeok.kafkaactiveactive.publish.Event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.Utils;

/** Shared SDK wire format. This is an internal implementation API. */
public final class EventCodec {
    public static final String EVENT_ID_HEADER = "kaa.event.id";
    public static final String FORMAT_HEADER = "kaa.format";
    private EventCodec() {}

    public static ProducerRecord<byte[], byte[]> encode(String topic, Event event) {
        return new ProducerRecord<>(topic, event.partition(), null,
                event.key().getBytes(StandardCharsets.UTF_8), event.payload(), List.of(
                new RecordHeader(EVENT_ID_HEADER, event.eventId().getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(FORMAT_HEADER, new byte[] {1})));
    }

    public static Event event(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        Identity identity = identity(record, maxBytes);
        return new Event(identity.id(), text(record.key()), record.value());
    }

    public static String utf8(byte[] value) { return text(value); }

    public static Identity identity(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        byte[] id = uniqueHeader(record, EVENT_ID_HEADER);
        byte[] version = uniqueHeader(record, FORMAT_HEADER);
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
            return new Identity(eventId, digest.digest());
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

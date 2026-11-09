package io.github.keemgdeok.kafkaactiveactive.replication;

import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;

/** Target append ACK evidence. Source log observation is not a source Produce ACK. */
public record ReplicationAck(UUID attempt, String eventId, byte[] hash, int partition,
                             LogIdentity source, long sourceOffset, LogIdentity target, long targetOffset) {
    public ReplicationAck {
        Objects.requireNonNull(attempt, "attempt");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        hash = hash.clone();
        if (eventId.isBlank() || !StandardCharsets.UTF_8.newEncoder().canEncode(eventId)
                || hash.length != 32 || partition < 0 || partition > 1
                || sourceOffset < 0 || targetOffset < 0 || source.clusterId().equals(target.clusterId())) {
            throw new IllegalArgumentException("invalid replication ACK evidence");
        }
    }
    @Override public byte[] hash() { return hash.clone(); }

    public ProducerRecord<byte[], byte[]> record(String topic) {
        byte[][] text = {bytes(eventId), bytes(source.clusterId()), bytes(source.topicId()),
                bytes(target.clusterId()), bytes(target.topicId())};
        int size = 2 + 32 + 16;
        for (byte[] value : text) { size = Math.addExact(size, 4 + value.length); }
        var buffer = ByteBuffer.allocate(size);
        buffer.put((byte) 1).put((byte) partition);
        put(buffer, text[0]); buffer.put(hash);
        put(buffer, text[1]); put(buffer, text[2]); buffer.putLong(sourceOffset);
        put(buffer, text[3]); put(buffer, text[4]); buffer.putLong(targetOffset);
        return new ProducerRecord<>(topic, partition, EventCodec.uuid(attempt), buffer.array());
    }
    public static ReplicationAck decode(ConsumerRecord<byte[], byte[]> record, int maxEventBytes) {
        if (record.key() == null || record.key().length != 16 || record.value() == null
                || record.value().length > (long) maxEventBytes + 1_024) {
            throw new IllegalArgumentException("invalid ACK record size or key");
        }
        try {
            var key = ByteBuffer.wrap(record.key());
            var attempt = new UUID(key.getLong(), key.getLong());
            var buffer = ByteBuffer.wrap(record.value());
            if (buffer.get() != 1) { throw new IllegalArgumentException("unsupported ACK format"); }
            int partition = Byte.toUnsignedInt(buffer.get());
            String id = text(buffer, maxEventBytes);
            byte[] hash = new byte[32]; buffer.get(hash);
            var source = new LogIdentity(text(buffer, 256), text(buffer, 256));
            long sourceOffset = buffer.getLong();
            var target = new LogIdentity(text(buffer, 256), text(buffer, 256));
            long targetOffset = buffer.getLong();
            if (buffer.hasRemaining() || partition != record.partition()) {
                throw new IllegalArgumentException("ACK partition mismatch or trailing bytes");
            }
            return new ReplicationAck(attempt, id, hash, partition, source, sourceOffset, target, targetOffset);
        } catch (BufferUnderflowException malformed) {
            throw new IllegalArgumentException("truncated ACK", malformed);
        }
    }
    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    private static void put(ByteBuffer buffer, byte[] bytes) { buffer.putInt(bytes.length).put(bytes); }
    private static String text(ByteBuffer buffer, int max) {
        int length = buffer.getInt();
        if (length < 1 || length > max || length > buffer.remaining()) {
            throw new IllegalArgumentException("invalid ACK text length");
        }
        byte[] bytes = new byte[length]; buffer.get(bytes);
        return EventCodec.utf8(bytes);
    }
}

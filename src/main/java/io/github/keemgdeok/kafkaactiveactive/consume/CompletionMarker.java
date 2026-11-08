package io.github.keemgdeok.kafkaactiveactive.consume;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

record CompletionMarker(String id, int partition, byte[] hash) {
    CompletionMarker { hash = hash.clone(); }
    @Override public byte[] hash() { return hash.clone(); }
    boolean matches(CompletionMarker other) {
        return partition == other.partition && Arrays.equals(hash, other.hash);
    }
    static CompletionMarker event(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        var identity = EventCodec.identity(record, maxBytes);
        return new CompletionMarker(identity.id(), record.partition(), identity.hash());
    }
    static CompletionMarker decode(ConsumerRecord<byte[], byte[]> record, int maxBytes) {
        if (record.key() == null || record.value() == null || record.key().length > maxBytes
                || record.value().length != 37) {
            throw new IllegalStateException("invalid completion marker or tombstone");
        }
        var buffer = ByteBuffer.wrap(record.value());
        if (buffer.get() != 1) { throw new IllegalStateException("unsupported completion format"); }
        int partition = buffer.getInt();
        byte[] hash = new byte[32];
        buffer.get(hash);
        String id = EventCodec.utf8(record.key());
        if (id.isBlank() || partition != record.partition() || partition < 0 || partition > 1) {
            throw new IllegalStateException("invalid completion identity/partition");
        }
        return new CompletionMarker(id, partition, hash);
    }
    ProducerRecord<byte[], byte[]> record(String topic) {
        return new ProducerRecord<>(topic, partition, id.getBytes(StandardCharsets.UTF_8),
                ByteBuffer.allocate(37).put((byte) 1).putInt(partition).put(hash).array());
    }
}

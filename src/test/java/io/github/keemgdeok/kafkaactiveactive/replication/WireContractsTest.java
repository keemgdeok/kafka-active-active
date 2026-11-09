package io.github.keemgdeok.kafkaactiveactive.replication;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

class WireContractsTest {
    private final Event event = new Event("event-한글", "hello", new byte[] {0, -1, 1});
    private final LogIdentity source = new LogIdentity("cluster-a", "topic-a");
    private final LogIdentity target = new LogIdentity("cluster-b", "topic-b");

    @Test void legacyRawAndReplicaPreserveBusinessIdentityAndHash() {
        var attempt = UUID.randomUUID();
        var legacy = consumed(EventCodec.encode("events", event));
        var raw = consumed(EventCodec.raw("events", event, attempt));
        var replica = consumed(EventCodec.replica("events", raw, 1_024));
        assertEquals(EventCodec.Kind.LEGACY, EventCodec.kind(legacy));
        assertEquals(EventCodec.Kind.RAW, EventCodec.kind(raw));
        assertEquals(EventCodec.Kind.REPLICA, EventCodec.kind(replica));
        assertEquals(attempt, EventCodec.attempt(raw));
        for (var record : java.util.List.of(legacy, raw, replica)) {
            assertEquals(event, EventCodec.event(record, 1_024));
            assertArrayEquals(EventCodec.identity(legacy, 1_024).hash(), EventCodec.identity(record, 1_024).hash());
            assertEquals(event.partition(), record.partition());
        }
        assertNull(replica.headers().lastHeader(EventCodec.ATTEMPT_HEADER));
        assertEquals(EventCodec.Kind.REPLICA, EventCodec.kind(consumed(EventCodec.replica("events", legacy, 1_024))));
    }

    @Test void malformedKindsAttemptsAndDuplicatedHeadersFailClosed() {
        var raw = consumed(EventCodec.raw("events", event, UUID.randomUUID()));
        raw.headers().remove(EventCodec.ATTEMPT_HEADER);
        assertThrows(IllegalArgumentException.class, () -> EventCodec.identity(raw, 1_024));
        raw.headers().add(EventCodec.ATTEMPT_HEADER, new byte[15]);
        assertThrows(IllegalArgumentException.class, () -> EventCodec.identity(raw, 1_024));
        var repeated = consumed(EventCodec.raw("events", event, UUID.randomUUID()));
        repeated.headers().add(new RecordHeader(EventCodec.KIND_HEADER, new byte[] {0}));
        assertThrows(IllegalArgumentException.class, () -> EventCodec.identity(repeated, 1_024));
        var unknown = consumed(EventCodec.raw("events", event, UUID.randomUUID()));
        unknown.headers().remove(EventCodec.KIND_HEADER).add(EventCodec.KIND_HEADER, new byte[] {3});
        assertThrows(IllegalArgumentException.class, () -> EventCodec.identity(unknown, 1_024));
    }

    @Test void ackRoundTripsWithNativeIdentitiesAndIndependentOffsets() {
        var ack = acknowledgement();
        var result = ReplicationAck.decode(consumed(ack.record("acks")), 1_024);
        assertEquals(ack.attempt(), result.attempt());
        assertEquals(event.eventId(), result.eventId());
        assertEquals(ack.source(), result.source());
        assertEquals(ack.target(), result.target());
        assertEquals(70, result.sourceOffset());
        assertEquals(8, result.targetOffset());
        assertArrayEquals(ack.hash(), result.hash());
        result.hash()[0]++;
        assertArrayEquals(ack.hash(), result.hash());
    }

    @Test void malformedAckCannotSupplyEvidenceOrAllocateFromUntrustedLengths() {
        var original = acknowledgement().record("acks");
        for (int size : new int[] {0, 1, 10, original.value().length - 1}) {
            var cut = new ProducerRecord<byte[], byte[]>("acks", event.partition(), original.key(),
                    Arrays.copyOf(original.value(), size));
            assertThrows(IllegalArgumentException.class, () -> ReplicationAck.decode(consumed(cut), 1_024));
        }
        var invalid = original.value().clone();
        ByteBuffer.wrap(invalid, 2, 4).putInt(Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, () -> ReplicationAck.decode(
                consumed(new ProducerRecord<>("acks", event.partition(), original.key(), invalid)), 1_024));
        var mismatched = new ProducerRecord<>("acks", 1 - event.partition(), original.key(), original.value());
        assertThrows(IllegalArgumentException.class, () -> ReplicationAck.decode(consumed(mismatched), 1_024));
        assertThrows(IllegalArgumentException.class, () -> new ReplicationAck(UUID.randomUUID(), "id", new byte[32],
                0, source, 0, source, 0));
    }

    @Test void ackSupportsLongUtf8IdsWithoutModifiedUtfLengthTruncation() {
        String id = "한".repeat(24_000);
        var ack = new ReplicationAck(UUID.randomUUID(), id, new byte[32], 0, source, 0, target, 0);
        assertEquals(id, ReplicationAck.decode(consumed(ack.record("acks")), 100_000).eventId());
        assertThrows(IllegalArgumentException.class,
                () -> ReplicationAck.decode(consumed(ack.record("acks")), 1_024));
        assertThrows(IllegalArgumentException.class, () -> new LogIdentity("한".repeat(100), "topic"));
        assertThrows(IllegalArgumentException.class,
                () -> new ReplicationAck(UUID.randomUUID(), "bad\uD800", new byte[32], 0, source, 0, target, 0));
    }

    @Test void progressNamespaceChangesWhenEitherLogIsRecreated() {
        var config = ReplicationConfig.defaults("events");
        assertEquals(config.groupId(source, target), config.groupId(source, target));
        assertNotEquals(config.groupId(source, target),
                config.groupId(source, new LogIdentity("cluster-b", "new-topic")));
        assertNotEquals(config.transactionalId(source, target),
                config.transactionalId(new LogIdentity("new-a", "topic-a"), target));
        assertThrows(IllegalArgumentException.class, () -> config.groupId(source, source));
        assertEquals(86, config.confirmationTopic().length());
    }

    @Test void acknowledgementEvidenceAndPreferredDeadlineCannotBeWeakened() {
        var config = PublishConfig.defaults("events");
        assertEquals(Duration.ofSeconds(1), config.preferredAckTimeout());
        assertThrows(IllegalArgumentException.class, () -> new PublishConfig("events", Duration.ofSeconds(1),
                1, 1, Duration.ofSeconds(1), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new PublishResult.BrokerResult(
                PublishResult.Delivery.ACKNOWLEDGED, 1L, null, PublishResult.Evidence.NONE));
        assertThrows(IllegalArgumentException.class, () -> new PublishResult.BrokerResult(
                PublishResult.Delivery.UNCONFIRMED, null, null, PublishResult.Evidence.REPLICATION_ACK));
    }

    private ReplicationAck acknowledgement() {
        return new ReplicationAck(UUID.randomUUID(), event.eventId(), EventCodec.hash(event.key(), event.payload()),
                event.partition(), source, 70, target, 8);
    }

    private static ConsumerRecord<byte[], byte[]> consumed(ProducerRecord<byte[], byte[]> record) {
        return new ConsumerRecord<>(record.topic(), record.partition(), 0, 0, TimestampType.CREATE_TIME,
                record.key().length, record.value().length, record.key(), record.value(), record.headers(), Optional.empty());
    }
}

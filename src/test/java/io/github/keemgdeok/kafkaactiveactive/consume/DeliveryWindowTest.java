package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class DeliveryWindowTest {
    private final ConsumerSettings settings = ConsumerSettings.defaults("events", "group");

    @Test void duplicateIdsInOnePollAreDeliveredOnceAndAllTheirOffsetsAdvanceTogether() {
        var first = record("one", "kafka", 0, new byte[] {1});
        var duplicate = record("one", "kafka", 1, new byte[] {1});
        var window = new DeliveryWindow(List.of(first, duplicate), Map.of(0, 2L), new CompletedIndex(settings), 1_024);
        assertEquals(1, window.batch().records().size());
        var plan = window.plan(window.batch().processed());
        assertEquals(2, plan.next().get(0));
        assertEquals(1, plan.markers().get(0).size());
    }

    @Test void partialPrefixNeverCommitsPastUnfinishedRecordEvenWithCompletedRecordsAfterIt() {
        var index = new CompletedIndex(settings);
        var one = record("one", "kafka", 0, new byte[] {1});
        var two = record("two", "kafka", 1, new byte[] {2});
        var three = record("three", "kafka", 2, new byte[] {3});
        index.add(CompletionMarker.event(three, 1_024));
        var window = new DeliveryWindow(List.of(one, two, three), Map.of(0, 3L), index, 1_024);
        assertEquals(2, window.batch().records().size());
        var plan = window.plan(window.batch().processedPrefix(1));
        assertEquals(1, plan.next().get(0));
        assertEquals(1, plan.rewind().get(0));
        assertEquals("one", plan.markers().get(0).getFirst().id());
    }

    @Test void eachPartitionAdvancesIndependentlyWithoutSkippingAHole() {
        var a = record("a", "kafka", 0, new byte[] {1});
        var b = record("b", "hello", 0, new byte[] {2});
        var window = new DeliveryWindow(List.of(a, b), Map.of(0, 1L, 1, 1L), new CompletedIndex(settings), 1_024);
        var plan = window.plan(window.batch().processedPrefix(1));
        assertEquals(1, plan.next().get(0));
        assertEquals(0, plan.next().get(1));
        assertFalse(plan.rewind().containsKey(0));
        assertEquals(0, plan.rewind().get(1));
    }

    @Test void conflictingPayloadIsRejectedBeforeDeliveryAndCompletedMarkerCannotHideIt() {
        var first = record("same", "kafka", 0, new byte[] {1});
        var second = record("same", "kafka", 1, new byte[] {2});
        assertThrows(IllegalStateException.class, () -> new DeliveryWindow(List.of(first, second),
                Map.of(0, 2L), new CompletedIndex(settings), 1_024));
        var index = new CompletedIndex(settings);
        index.add(CompletionMarker.event(first, 1_024));
        assertThrows(IllegalStateException.class, () -> new DeliveryWindow(List.of(second), Map.of(0, 2L), index, 1_024));
    }

    @Test void completedIndexNeverEvictsAndReservesCapacityBeforeTransaction() {
        var limits = new ConsumerSettings("events", "group", java.time.Duration.ofSeconds(1), 10, 1, 1_024, 1_024);
        var index = new CompletedIndex(limits);
        var one = CompletionMarker.event(record("one", "kafka", 0, new byte[] {1}), 1_024);
        var two = CompletionMarker.event(record("two", "kafka", 1, new byte[] {2}), 1_024);
        index.add(one);
        assertThrows(IllegalStateException.class, () -> index.reserve(List.of(two)));
        assertTrue(index.contains(one));
        assertEquals(1, index.size());
    }

    @Test void staleBatchTokenAndInvalidPrefixAreRejected() {
        var record = record("one", "kafka", 0, new byte[] {1});
        var a = new DeliveryWindow(List.of(record), Map.of(0, 1L), new CompletedIndex(settings), 1_024);
        var b = new DeliveryWindow(List.of(record), Map.of(0, 1L), new CompletedIndex(settings), 1_024);
        assertThrows(IllegalArgumentException.class, () -> a.plan(b.batch().processed()));
        assertThrows(IllegalArgumentException.class, () -> a.batch().processedPrefix(2));
    }

    static ConsumerRecord<byte[], byte[]> record(String id, String key, long offset, byte[] payload) {
        var encoded = EventCodec.encode("events", new Event(id, key, payload));
        var record = new ConsumerRecord<>("events", encoded.partition(), offset, encoded.key(), encoded.value());
        encoded.headers().forEach(header -> record.headers().add(header));
        return record;
    }
}

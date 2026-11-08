package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class CompletionMarkerTest {
    @Test void versionedCompletionRoundTripsWithoutDependingOnInputOffsetNumbers() {
        var original = CompletionMarker.event(DeliveryWindowTest.record("id-한글", "hello", 190,
                new byte[] {0, -1, 1}), 1_024);
        var encoded = original.record("ledger");
        var decoded = CompletionMarker.decode(new ConsumerRecord<>("ledger", 1, 7, encoded.key(), encoded.value()), 1_024);
        assertEquals(original.id(), decoded.id());
        assertTrue(original.matches(decoded));
        decoded.hash()[0]++;
        assertTrue(original.matches(decoded));
    }

    @Test void tombstonesAndUnsupportedVersionsCannotEraseCompletionHistorySilently() {
        assertThrows(IllegalStateException.class, () -> CompletionMarker.decode(
                new ConsumerRecord<>("ledger", 0, 0, new byte[] {'i'}, null), 1_024));
        byte[] invalid = new byte[37];
        invalid[0] = 2;
        assertThrows(IllegalStateException.class, () -> CompletionMarker.decode(
                new ConsumerRecord<>("ledger", 0, 0, new byte[] {'i'}, invalid), 1_024));
    }

    @Test void byteBudgetIsCheckedBeforeAddingOrReservingACompletion() {
        var settings = new ConsumerSettings("events", "group", Duration.ofSeconds(1), 10, 100, 1, 1_024);
        var index = new CompletedIndex(settings);
        var marker = CompletionMarker.event(DeliveryWindowTest.record("one", "kafka", 0, new byte[] {1}), 1_024);
        assertThrows(IllegalStateException.class, () -> index.reserve(List.of(marker)));
        assertThrows(IllegalStateException.class, () -> index.add(marker));
        assertEquals(0, index.size());
    }

    @Test void namespacesAreStableAndIsolateTopicGroupAndPartition() {
        var a = ConsumerSettings.defaults("events", "a");
        assertEquals(a.completionTopic(), ConsumerSettings.defaults("events", "a").completionTopic());
        assertNotEquals(a.completionTopic(), ConsumerSettings.defaults("events", "b").completionTopic());
        assertNotEquals(a.completionTopic(), ConsumerSettings.defaults("other", "a").completionTopic());
        assertNotEquals(a.transactionalId(0), a.transactionalId(1));
        assertThrows(IllegalArgumentException.class, () -> a.transactionalId(2));
    }
}

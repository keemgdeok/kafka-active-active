package io.github.keemgdeok.kafkaactiveactive.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class EventDatasetTest {
    @Test void sameSeedProducesExactlyTheSameIdsKeysAndBinaryPayloadsAcrossRuns() {
        assertEquals(EventDataset.generate(100, 7, false), EventDataset.generate(100, 7, false));
        assertNotEquals(EventDataset.generate(100, 7, false), EventDataset.generate(100, 8, false));
    }
    @Test void hotKeyScenarioTargetsOnePartitionAndKeepsEventIdsUnique() {
        var events = EventDataset.generate(100, 0, true);
        assertEquals(100, events.stream().map(event -> event.eventId()).distinct().count());
        assertEquals(1, events.stream().map(event -> event.partition()).distinct().count());
        assertTrue(events.stream().allMatch(event -> event.key().equals("hot-key")));
    }
}

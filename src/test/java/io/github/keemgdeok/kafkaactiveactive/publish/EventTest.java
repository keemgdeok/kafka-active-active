package io.github.keemgdeok.kafkaactiveactive.publish;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class EventTest {
    @Test
    void payloadIsDefensivelyCopiedOnInputAndOutput() {
        byte[] bytes = {1, 2};
        Event event = new Event("id", "key", bytes);
        bytes[0] = 9;
        event.payload()[1] = 9;
        assertArrayEquals(new byte[] {1, 2}, event.payload());
        assertEquals(new Event("id", "key", new byte[] {1, 2}), event);
        assertEquals(new Event("id", "key", new byte[] {1, 2}).hashCode(), event.hashCode());
    }

    @Test
    void murmur2PartitionMatchesKnownKafkaVectorsIncludingUtf8() {
        assertEquals(0, new Event("id", "kafka", new byte[0]).partition());
        assertEquals(1, new Event("id", "hello", new byte[0]).partition());
        assertEquals(1, new Event("id", "한글", new byte[0]).partition());
    }

    @Test
    void rejectsInvalidIdentityAndUnboundedConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new Event("", "key", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new Event("id", " ", new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> new Event("id\uD800", "key", new byte[0]));
        assertThrows(NullPointerException.class, () -> new Event("id", "key", null));
        assertThrows(IllegalArgumentException.class, () -> new PublishConfig(
                "events", Duration.ZERO, 1, 1, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> new PublishConfig(
                "events", Duration.ofSeconds(1), 0, 1, Duration.ofSeconds(1)));
    }
}

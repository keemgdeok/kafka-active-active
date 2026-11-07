package io.github.keemgdeok.kafkaactiveactive.publish;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import org.apache.kafka.common.utils.Utils;

/** A caller-assigned identity. Application retries must reuse the ID, key and payload. */
public record Event(String eventId, String key, byte[] payload) {
    public Event {
        if (eventId == null || eventId.isBlank() || key == null || key.isBlank()) {
            throw new IllegalArgumentException("eventId and key must not be blank");
        }
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(eventId)
                || !StandardCharsets.UTF_8.newEncoder().canEncode(key)) {
            throw new IllegalArgumentException("eventId and key must be valid Unicode");
        }
        payload = Objects.requireNonNull(payload, "payload").clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Event event && eventId.equals(event.eventId)
                && key.equals(event.key) && Arrays.equals(payload, event.payload);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(eventId, key) + Arrays.hashCode(payload);
    }

    public int partition() {
        return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % 2;
    }

    long sizeBytes() {
        return (long) eventId.getBytes(StandardCharsets.UTF_8).length
                + key.getBytes(StandardCharsets.UTF_8).length + payload.length;
    }
}

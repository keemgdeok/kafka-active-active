package io.github.keemgdeok.kafkaactiveactive.verification;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.github.keemgdeok.kafkaactiveactive.publish.Event;

/** Deterministic input shared by demo, fault verification and baseline comparison. */
public final class EventDataset {
    private EventDataset() {}
    public static List<Event> generate(int count, long seed, boolean hotKey) {
        if (count < 1 || count > 100_000) { throw new IllegalArgumentException("count must be 1..100000"); }
        Random random = new Random(seed);
        List<Event> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String key = hotKey ? "hot-key" : "customer-" + random.nextInt(32);
            byte[] payload = ByteBuffer.allocate(24).putLong(seed).putLong(i).putLong(random.nextLong()).array();
            events.add(new Event("seed-" + seed + "-event-" + i, key, payload));
        }
        return List.copyOf(events);
    }
}

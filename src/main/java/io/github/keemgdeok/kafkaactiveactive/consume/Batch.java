package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.List;
import java.util.UUID;

import io.github.keemgdeok.kafkaactiveactive.publish.Event;

/** Only one batch may be outstanding. Process a prefix; commit never skips an unfinished record. */
public final class Batch {
    private final UUID token = UUID.randomUUID();
    private final List<Delivery> records;
    Batch(List<Delivery> records) { this.records = List.copyOf(records); }
    public List<Delivery> records() { return records; }
    public boolean isEmpty() { return records.isEmpty(); }
    public ProcessedBatch processed() { return processedPrefix(records.size()); }
    public ProcessedBatch processedPrefix(int count) {
        if (count < 0 || count > records.size()) { throw new IllegalArgumentException("invalid processed prefix"); }
        return new ProcessedBatch(token, count);
    }
    boolean owns(ProcessedBatch processed) { return token.equals(processed.token()); }
    public record Delivery(Event event, int partition, long offset) {}
}

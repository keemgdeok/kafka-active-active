package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.UUID;

public final class ProcessedBatch {
    private final UUID token;
    private final int count;
    ProcessedBatch(UUID token, int count) { this.token = token; this.count = count; }
    UUID token() { return token; }
    int count() { return count; }
}

package io.github.keemgdeok.kafkaactiveactive.repair;

import java.time.Duration;
import java.util.Objects;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;

/** Finite full-log reconciliation; index byte accounting is a conservative estimate, not heap usage. */
public record RepairConfig(String topic, Duration runTimeout, Duration interval,
                           int maxIndexEntries, long maxIndexBytes, int maxEventBytes) {
    public RepairConfig {
        new PublishConfig(topic, Duration.ofSeconds(1), 1, maxEventBytes, Duration.ofSeconds(1));
        for (Duration value : new Duration[] {runTimeout, interval}) {
            Objects.requireNonNull(value, "duration");
            if (value.toMillis() < 1 || value.compareTo(Duration.ofDays(1)) > 0) {
                throw new IllegalArgumentException("durations must be 1ms..1day");
            }
        }
        if (maxIndexEntries < 1 || maxIndexBytes < 1) {
            throw new IllegalArgumentException("index limits must be positive");
        }
    }

    public static RepairConfig defaults(String topic) {
        return new RepairConfig(topic, Duration.ofMinutes(5), Duration.ofSeconds(30),
                1_000_000, 256L * 1024 * 1024, 900_000);
    }
}

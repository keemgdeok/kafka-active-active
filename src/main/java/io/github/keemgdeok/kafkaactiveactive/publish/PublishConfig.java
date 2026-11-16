package io.github.keemgdeok.kafkaactiveactive.publish;

import java.time.Duration;
import java.util.Objects;
import io.github.keemgdeok.kafkaactiveactive.internal.EventConstraints;

public record PublishConfig(String topic, Duration timeout, int maxPending, int maxEventBytes,
                            Duration closeTimeout, Duration preferredAckTimeout) {
    public PublishConfig(String topic, Duration timeout, int maxPending, int maxEventBytes, Duration closeTimeout) {
        this(topic, timeout, maxPending, maxEventBytes, closeTimeout,
                Duration.ofNanos(Math.min(Duration.ofSeconds(1).toNanos(), timeout.toNanos() / 3)));
    }
    public PublishConfig {
        EventConstraints.validate(topic, maxEventBytes);
        requireDuration(timeout, "timeout");
        requireDuration(closeTimeout, "closeTimeout");
        Objects.requireNonNull(preferredAckTimeout, "preferredAckTimeout");
        if (preferredAckTimeout.isZero() || preferredAckTimeout.isNegative()
                || preferredAckTimeout.compareTo(timeout) >= 0) {
            throw new IllegalArgumentException("preferredAckTimeout must be positive and less than timeout");
        }
        if (maxPending < 1) {
            throw new IllegalArgumentException("maxPending must be positive");
        }
    }

    public static PublishConfig defaults(String topic) {
        return new PublishConfig(topic, Duration.ofSeconds(5), 128, EventConstraints.DEFAULT_MAX_EVENT_BYTES,
                Duration.ofSeconds(5));
    }

    private static void requireDuration(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.toMillis() < 1 || value.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException(name + " must be between 1ms and 5min");
        }
    }
}

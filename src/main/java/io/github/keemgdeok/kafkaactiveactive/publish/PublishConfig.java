package io.github.keemgdeok.kafkaactiveactive.publish;

import java.time.Duration;
import java.util.Objects;

/** Bounds apply to admitted requests, including requests still pending after a response timeout. */
public record PublishConfig(String topic, Duration timeout, int maxPending, int maxEventBytes,
                            Duration closeTimeout, Duration preferredAckTimeout) {
    public PublishConfig(String topic, Duration timeout, int maxPending, int maxEventBytes, Duration closeTimeout) {
        this(topic, timeout, maxPending, maxEventBytes, closeTimeout,
                Duration.ofNanos(Math.min(Duration.ofSeconds(1).toNanos(), timeout.toNanos() / 3)));
    }
    public PublishConfig {
        if (topic == null || !topic.matches("[a-zA-Z0-9._-]{1,249}")
                || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("invalid Kafka topic name");
        }
        requireDuration(timeout, "timeout");
        requireDuration(closeTimeout, "closeTimeout");
        Objects.requireNonNull(preferredAckTimeout, "preferredAckTimeout");
        if (preferredAckTimeout.isZero() || preferredAckTimeout.isNegative()
                || preferredAckTimeout.compareTo(timeout) >= 0) {
            throw new IllegalArgumentException("preferredAckTimeout must be positive and less than timeout");
        }
        if (maxPending < 1 || maxEventBytes < 1 || maxEventBytes > 16 * 1024 * 1024) {
            throw new IllegalArgumentException("maxPending must be positive; maxEventBytes must be 1..16MiB");
        }
    }

    public static PublishConfig defaults(String topic) {
        return new PublishConfig(topic, Duration.ofSeconds(5), 128, 900_000, Duration.ofSeconds(5));
    }

    private static void requireDuration(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.toMillis() < 1 || value.compareTo(Duration.ofMinutes(5)) > 0) {
            throw new IllegalArgumentException(name + " must be between 1ms and 5min");
        }
    }
}

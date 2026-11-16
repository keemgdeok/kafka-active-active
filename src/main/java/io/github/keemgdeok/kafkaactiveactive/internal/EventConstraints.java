package io.github.keemgdeok.kafkaactiveactive.internal;

import org.apache.kafka.common.utils.Utils;

public final class EventConstraints {
    public static final int PARTITION_COUNT = 2;
    public static final int DEFAULT_MAX_EVENT_BYTES = 900_000;
    public static final int RECORD_OVERHEAD_BYTES = 1_024;
    public static final int MAX_EVENT_BYTES = 16 * 1024 * 1024;

    private EventConstraints() {}

    public static void validate(String topic, int maxEventBytes) {
        if (topic == null || !topic.matches("[a-zA-Z0-9._-]{1,249}")
                || topic.equals(".") || topic.equals("..")) {
            throw new IllegalArgumentException("invalid Kafka topic name");
        }
        if (maxEventBytes < 1 || maxEventBytes > MAX_EVENT_BYTES) {
            throw new IllegalArgumentException("maxEventBytes must be 1..16MiB");
        }
    }

    public static int partition(byte[] key) {
        return Utils.toPositive(Utils.murmur2(key)) % PARTITION_COUNT;
    }
}

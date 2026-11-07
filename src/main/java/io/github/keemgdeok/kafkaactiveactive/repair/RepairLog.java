package io.github.keemgdeok.kafkaactiveactive.repair;

import java.time.Duration;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;

interface RepairLog extends AutoCloseable {
    RepairReport.LogBoundary capture();
    void scan(RepairReport.LogBoundary boundary, Map<Integer, Long> from,
              Consumer<ConsumerRecord<byte[], byte[]>> visitor);
    void append(ConsumerRecord<byte[], byte[]> record);

    default void verify(RepairReport.LogBoundary boundary) {
        boundary.requireContinuation(capture());
    }

    @Override
    void close();

    final class Deadline {
        private final long end;
        private final LongSupplier clock;
        private final BooleanSupplier closed;

        Deadline(Duration timeout, LongSupplier clock, BooleanSupplier closed) {
            this.clock = clock;
            this.closed = closed;
            end = clock.getAsLong() + timeout.toNanos();
        }

        Duration remaining() {
            if (closed.getAsBoolean() || Thread.currentThread().isInterrupted()) {
                throw new RepairException(RepairException.Reason.CLOSED, "repair was closed or interrupted");
            }
            long remaining = end - clock.getAsLong();
            if (remaining <= 0) {
                throw new RepairException(RepairException.Reason.DEADLINE, "repair run deadline elapsed");
            }
            return Duration.ofNanos(remaining);
        }

        Duration operationTimeout() {
            return remaining().compareTo(Duration.ofSeconds(5)) < 0 ? remaining() : Duration.ofSeconds(5);
        }
    }
}

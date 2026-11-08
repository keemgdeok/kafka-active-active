package io.github.keemgdeok.kafkaactiveactive.repair;

import java.util.Map;
import java.util.function.Consumer;

import io.github.keemgdeok.kafkaactiveactive.support.CrashProcess;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Crashes after an actual target ACK, before any in-memory repair progress can survive. */
public final class RepairCrashChild {
    private RepairCrashChild() {}
    public static void main(String[] args) {
        var config = RepairConfig.defaults(args[0]);
        try (var repair = new RepairService(config,
                deadline -> new KafkaRepairLog(config, Map.of("bootstrap.servers", args[1]), deadline),
                deadline -> afterAck(new KafkaRepairLog(config, Map.of("bootstrap.servers", args[2]), deadline)), System::nanoTime)) {
            repair.runOnce();
            throw new AssertionError("repair did not copy a missing event");
        }
    }
    private static RepairLog afterAck(RepairLog delegate) {
        return new RepairLog() {
            @Override public RepairReport.LogBoundary capture() { return delegate.capture(); }
            @Override public void scan(RepairReport.LogBoundary boundary, Map<Integer, Long> from,
                                       Consumer<ConsumerRecord<byte[], byte[]>> visitor) { delegate.scan(boundary, from, visitor); }
            @Override public void append(ConsumerRecord<byte[], byte[]> record) {
                delegate.append(record);
                CrashProcess.halt("REPAIR_TARGET_ACK");
            }
            @Override public void close() { delegate.close(); }
        };
    }
}

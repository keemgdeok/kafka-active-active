package io.github.keemgdeok.kafkaactiveactive.replication;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.support.CrashProcess;

/** Actual lost process memory and orphan native transactions, not a thrown exception. */
public final class ReplicationCrashChild {
    private ReplicationCrashChild() {}
    static Event input(int seed) {
        return new Event("replication-crash-" + seed, seed % 2 == 0 ? "kafka" : "hello",
                ByteBuffer.allocate(8).putLong(seed).array());
    }
    public static void main(String[] args) throws InterruptedException {
        int seed = Integer.parseInt(args[4]);
        var stage = ReplicationService.Stage.valueOf(args[3]);
        try (var service = new ReplicationService(ReplicationConfig.defaults(args[0]),
                Map.of("bootstrap.servers", args[1]), Map.of("bootstrap.servers", args[2]), point -> {
                    if (point.sourceSide() == seed % 2 && point.stage() == stage) { CrashProcess.halt(stage.name()); }
                })) {
            service.start();
            long end = System.nanoTime() + Duration.ofSeconds(35).toNanos();
            while (System.nanoTime() < end) { Thread.sleep(20); }
            throw new AssertionError("worker never reached fault: " + service.status());
        }
    }
}

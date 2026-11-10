package io.github.keemgdeok.kafkaactiveactive.consume;

import java.time.Duration;
import java.util.Map;

import io.github.keemgdeok.kafkaactiveactive.support.CrashProcess;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.verification.EventDataset;

/** Test-only entry point; never part of the published SDK artifact. */
public final class ConsumerCrashChild {
    private ConsumerCrashChild() {}
    public static void main(String[] args) {
        var settings = new ConsumerSettings(args[0], args[1], Duration.ofSeconds(5), 100, 1_000, 1_000_000, 1_024);
        String point = args[4];
        int seed=Integer.parseInt(args[5]);
        boolean multi=point.equals("FIRST_PARTITION_COMMITTED") || point.equals("ALL_PARTITIONS_COMMITTED");
        var expected = multi ? multiInput(seed) : java.util.List.of(input(seed));
        var committedPartitions=new java.util.concurrent.atomic.AtomicInteger();
        try (var consumer = new ActiveKafkaConsumer(settings, Map.of("bootstrap.servers", args[2]),
                Map.of("bootstrap.servers", args[3]), (partition, reached) -> {
                    if (multi && reached==ActiveKafkaConsumer.CommitPoint.LOCAL_COMMITTED) {
                        int count=committedPartitions.incrementAndGet();
                        if (count==(point.equals("FIRST_PARTITION_COMMITTED")?1:2)) { CrashProcess.halt(point); }
                    }
                    if (reached.name().equals(point)) { CrashProcess.halt(point); }
                })) {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (System.nanoTime() < deadline) {
                var batch = consumer.poll(Duration.ofMillis(100));
                if (batch.isEmpty()) { continue; }
                if (batch.records().size() != expected.size()
                        || !new java.util.HashSet<>(expected).equals(batch.records().stream().map(Batch.Delivery::event)
                                .collect(java.util.stream.Collectors.toSet()))) {
                    throw new AssertionError("child handler input differs from seeded ID/key/payload");
                }
                for (var event:expected) { System.out.println("HANDLER " + event.eventId()); }
                if (point.equals("AFTER_HANDLER")) { CrashProcess.halt(point); }
                consumer.commit(batch.processed()).toCompletableFuture().join();
                throw new AssertionError("commit did not reach requested crash point: " + point);
            }
            throw new AssertionError("child did not receive seeded event");
        }
    }
    static java.util.List<Event> multiInput(int seed) {
        return java.util.stream.IntStream.range(0,4).mapToObj(i -> new Event("multi-"+seed+"-"+i,
                i<2?"kafka":"hello",java.nio.ByteBuffer.allocate(8).putInt(seed).putInt(i).array())).toList();
    }
    static Event input(int seed) {
        // Small consecutive Random seeds can share their first key. Explicitly alternate the
        // partition so the repeated crash windows exercise both stable transactional identities.
        return EventDataset.generate(32, seed, false).stream().filter(event -> event.partition() == seed % 2)
                .findFirst().orElseThrow();
    }
}

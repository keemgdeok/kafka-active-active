package io.github.keemgdeok.kafkaactiveactive.support;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.github.keemgdeok.kafkaactiveactive.consume.ActiveKafkaConsumer;
import io.github.keemgdeok.kafkaactiveactive.consume.CommitResult;
import io.github.keemgdeok.kafkaactiveactive.consume.ConsumerSettings;

/** Test worker that owns all native consumer calls and publishes immutable observations. */
public final class ManagedConsumer implements AutoCloseable {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicReference<Set<Integer>> assignment = new AtomicReference<>(Set.of());
    private final AtomicReference<CommitResult> lastCommit = new AtomicReference<>();
    private final Set<String> committed = ConcurrentHashMap.newKeySet();
    private final CountDownLatch delivered = new CountDownLatch(1);
    private final CountDownLatch release;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Future<?> task;

    public ManagedConsumer(ConsumerSettings settings, Map<String, Object> active, Map<String, Object> peer,
                           ConcurrentHashMap<String, AtomicInteger> calls, boolean holdFirstBatch) {
        release = new CountDownLatch(holdFirstBatch ? 1 : 0);
        task = executor.submit(() -> {
            try (var consumer = new ActiveKafkaConsumer(settings, active, peer)) {
                while (running.get()) {
                    var batch = consumer.poll(Duration.ofMillis(100));
                    assignment.set(consumer.status().assignedPartitions());
                    if (batch.isEmpty()) { continue; }
                    for (var record : batch.records()) {
                        calls.computeIfAbsent(record.event().eventId(), ignored -> new AtomicInteger()).incrementAndGet();
                    }
                    delivered.countDown();
                    release.await();
                    if (!running.get()) { break; }
                    var result = consumer.commit(batch.processed()).toCompletableFuture().join();
                    lastCommit.set(result);
                    committed.addAll(result.locallyCompletedIds());
                    if (result.status() == CommitResult.Status.UNKNOWN) { break; }
                }
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("test consumer interrupted", failure);
            } finally { assignment.set(Set.of()); stopped.set(true); }
        });
    }
    public Set<Integer> assignment() { assertHealthy(); return assignment.get(); }
    public boolean stopped() { return stopped.get(); }
    public CommitResult lastCommit() { assertHealthy(); return lastCommit.get(); }
    public Set<String> committedIds() { assertHealthy(); return Set.copyOf(committed); }
    public void release() { release.countDown(); }
    public void awaitDelivery() throws InterruptedException {
        if (!delivered.await(20, TimeUnit.SECONDS)) { assertHealthy(); throw new AssertionError("no delivery"); }
    }
    public void assertHealthy() {
        if (task.isDone()) {
            try { task.get(); }
            catch (Exception failure) { throw new AssertionError("consumer worker failed", failure); }
        }
    }
    @Override public void close() {
        running.set(false);
        release.countDown();
        try { task.get(30, TimeUnit.SECONDS); }
        catch (Exception failure) { throw new AssertionError("consumer worker did not stop cleanly", failure); }
        finally { executor.shutdownNow(); }
    }
}

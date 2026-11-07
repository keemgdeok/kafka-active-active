package io.github.keemgdeok.kafkaactiveactive.repair;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * One repair owner per topic. Each run rebuilds its index from Kafka; no offsets or IDs are persisted.
 * Normal publishing can continue, but concurrent repair owners are not coordinated across processes.
 */
public final class RepairService implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(RepairService.class.getName());
    private final RepairConfig config;
    private final Function<RepairLog.Deadline, RepairLog> openA;
    private final Function<RepairLog.Deadline, RepairLog> openB;
    private final LongSupplier clock;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ReentrantLock runLock = new ReentrantLock();
    private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("kaa-repair").factory());
    private final AtomicReference<Status> status = new AtomicReference<>(new Status(false, null, null));
    private boolean started;

    public RepairService(RepairConfig config, Map<String, Object> clusterA, Map<String, Object> clusterB) {
        this(config, factory(config, clusterA), factory(config, clusterB), System::nanoTime);
    }

    private static Function<RepairLog.Deadline, RepairLog> factory(RepairConfig config, Map<String, Object> values) {
        Map<String, Object> properties = Map.copyOf(values);
        if (!properties.containsKey("bootstrap.servers") || properties.containsKey("interceptor.classes")
                || properties.containsKey("transactional.id")) {
            throw new IllegalArgumentException("bootstrap.servers required; interceptors and transactions unsupported");
        }
        return deadline -> new KafkaRepairLog(config, properties, deadline);
    }

    RepairService(RepairConfig config, Function<RepairLog.Deadline, RepairLog> openA,
                  Function<RepairLog.Deadline, RepairLog> openB, LongSupplier clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.openA = Objects.requireNonNull(openA, "openA");
        this.openB = Objects.requireNonNull(openB, "openB");
        this.clock = Objects.requireNonNull(clock, "clock");
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    /** Optional periodic execution; failures remain visible in status and the next run starts fresh. */
    public synchronized void start() {
        requireOpen();
        if (!started) {
            started = true;
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    runOnce();
                } catch (RepairException expected) {
                    // runOnce records failures; BUSY means a manual run already owns the service.
                }
            }, 0, config.interval().toNanos(), TimeUnit.NANOSECONDS);
        }
    }

    public Status status() {
        return status.get();
    }

    public RepairReport runOnce() {
        requireOpen();
        if (!runLock.tryLock()) {
            throw new RepairException(RepairException.Reason.BUSY, "another repair run is active");
        }
        String runId = UUID.randomUUID().toString();
        Instant startedAt = Instant.now();
        long[] copied = new long[2];
        status.updateAndGet(previous -> new Status(true, previous.lastCompleted(), previous.lastFailure()));
        try {
            requireOpen();
            var deadline = new RepairLog.Deadline(config.runTimeout(), clock, closed::get);
            RepairReport report;
            try (RepairLog a = openA.apply(deadline); RepairLog b = openB.apply(deadline)) {
                var initialA = a.capture();
                var initialB = b.capture();
                requireRetainedBeginning(initialA);
                requireRetainedBeginning(initialB);
                LOG.log(System.Logger.Level.INFO, "repair {0} initial A={1}, B={2}", runId, initialA, initialB);
                if (initialA.clusterId().equals(initialB.clusterId())) {
                    throw new RepairException(RepairException.Reason.INVALID_TOPOLOGY,
                            "A and B identify the same Kafka cluster");
                }
                RepairIndex index = new RepairIndex(config);
                scan(a, initialA, Map.of(0, 0L, 1, 0L), index, 0, deadline);
                scan(b, initialB, Map.of(0, 0L, 1, 0L), index, 1, deadline);

                // One finite tail refresh reduces races with delayed dual writes without chasing a moving tail.
                var boundaryA = a.capture();
                var boundaryB = b.capture();
                initialA.requireContinuation(boundaryA);
                initialB.requireContinuation(boundaryB);
                LOG.log(System.Logger.Level.INFO, "repair {0} finite boundary A={1}, B={2}", runId, boundaryA, boundaryB);
                scan(a, boundaryA, ends(initialA), index, 0, deadline);
                scan(b, boundaryB, ends(initialB), index, 1, deadline);
                a.verify(boundaryA);
                b.verify(boundaryB);

                // Validate the whole bounded union before copying, including ID/key/payload conflicts.
                copy(a, b, boundaryA, index, 0, copied, deadline);
                copy(b, a, boundaryB, index, 1, copied, deadline);
                index.requireFullyCopied();
                a.verify(boundaryA);
                b.verify(boundaryB);
                deadline.remaining();
                report = new RepairReport(runId, startedAt, Instant.now(), boundaryA, boundaryB,
                        index.size(), index.records(), index.bytes(), copied[0], copied[1]);
            }
            status.set(new Status(false, report, null));
            LOG.log(System.Logger.Level.INFO, "repair completed: {0}", report);
            return report;
        } catch (RuntimeException failure) {
            RepairException classified = failure instanceof RepairException repair ? repair : new RepairException(
                    RepairException.Reason.CLIENT_FAILURE, "Kafka repair operation failed", failure);
            status.updateAndGet(previous -> new Status(false, previous.lastCompleted(),
                    new Failure(runId, classified.reason(), classified.getMessage(), Instant.now(), copied[0], copied[1])));
            LOG.log(System.Logger.Level.WARNING, "repair {0} failed: {1}; ACKed copies to A={2}, B={3}",
                    runId, classified.reason(), copied[0], copied[1]);
            throw classified;
        } finally {
            runLock.unlock();
        }
    }

    private static Map<Integer, Long> ends(RepairReport.LogBoundary boundary) {
        return Map.of(0, boundary.partitions().get(0).end(), 1, boundary.partitions().get(1).end());
    }

    private static void requireRetainedBeginning(RepairReport.LogBoundary boundary) {
        if (!boundary.partitions().keySet().equals(java.util.Set.of(0, 1))) {
            throw new RepairException(RepairException.Reason.INVALID_TOPOLOGY, "expected partitions 0 and 1");
        }
        if (boundary.partitions().values().stream().anyMatch(p -> p.beginning() != 0
                || p.end() < 0 || p.leaderEpoch() < 0)) {
            throw new RepairException(RepairException.Reason.LOG_CHANGED, "retained beginning or epoch is unavailable");
        }
    }

    private static void scan(RepairLog log, RepairReport.LogBoundary boundary, Map<Integer, Long> from,
                             RepairIndex index, int side, RepairLog.Deadline deadline) {
        log.scan(boundary, from, record -> {
            deadline.remaining();
            index.add(record, side);
        });
        log.verify(boundary);
    }

    private static void copy(RepairLog source, RepairLog target, RepairReport.LogBoundary boundary,
                             RepairIndex index, int side, long[] copied, RepairLog.Deadline deadline) {
        source.scan(boundary, Map.of(0, 0L, 1, 0L), record -> {
            deadline.remaining();
            if (index.missingOnOtherSide(record, side)) {
                target.append(record);
                index.copied(record, 1 - side);
                copied[1 - side]++;
            }
        });
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new RepairException(RepairException.Reason.CLOSED, "repair service is closed");
        }
    }

    @Override
    public synchronized void close() {
        closed.set(true);
        scheduler.shutdownNow();
        try {
            if (!runLock.tryLock(5, TimeUnit.SECONDS)) {
                throw new RepairException(RepairException.Reason.BUSY, "active repair did not stop within 5 seconds");
            }
            runLock.unlock();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RepairException(RepairException.Reason.CLOSED, "interrupted while awaiting repair shutdown", failure);
        }
    }

    /** lastCompleted is historical boundary evidence; inspect lastFailure before treating the service as healthy. */
    public record Status(boolean running, RepairReport lastCompleted, Failure lastFailure) {}
    public record Failure(String runId, RepairException.Reason reason, String message, Instant occurredAt,
                          long copiedToA, long copiedToB) {}
}

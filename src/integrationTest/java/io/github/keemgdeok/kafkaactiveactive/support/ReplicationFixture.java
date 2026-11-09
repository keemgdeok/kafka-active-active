package io.github.keemgdeok.kafkaactiveactive.support;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService;

/** Tests explicitly own replication; this helper never starts it as a publisher side effect. */
public final class ReplicationFixture {
    private ReplicationFixture() {}
    public static ReplicationService start(String topic, Map<String, Object> a, Map<String, Object> b) {
        var service = new ReplicationService(ReplicationConfig.defaults(topic), a, b);
        service.start();
        long end = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < end) {
            var status = service.status();
            if (status.aToB().state() == ReplicationService.State.RUNNING
                    && status.bToA().state() == ReplicationService.State.RUNNING) { return service; }
            if (status.aToB().state() == ReplicationService.State.FAILED
                    || status.bToA().state() == ReplicationService.State.FAILED) { break; }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }
        var status = service.status(); service.close();
        throw new AssertionError("replication not ready: " + status);
    }
    public static RoutedKafkaPublisher ready(RoutedKafkaPublisher publisher) {
        long end = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < end) {
            if (publisher.status().clusterAConfirmationReady() && publisher.status().clusterBConfirmationReady()) { return publisher; }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }
        var status = publisher.status(); publisher.close();
        throw new AssertionError("confirmation readers not ready: " + status);
    }
}

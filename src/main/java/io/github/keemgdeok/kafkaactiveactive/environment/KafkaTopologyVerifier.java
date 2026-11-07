package io.github.keemgdeok.kafkaactiveactive.environment;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.KafkaFuture;

/** Verifies that the two bootstrap configurations identify independent Kafka clusters. */
public final class KafkaTopologyVerifier {
    private KafkaTopologyVerifier() {}

    public static Topology verify(String bootstrapA, String bootstrapB, Duration timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        requireBootstrap(bootstrapA);
        requireBootstrap(bootstrapB);
        Objects.requireNonNull(timeout, "timeout");
        long timeoutMillis = timeout.toMillis();
        if (timeoutMillis < 1 || timeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("timeout must be between 1ms and Integer.MAX_VALUE ms");
        }

        Admin a = Admin.create(properties(bootstrapA, (int) timeoutMillis));
        try {
            Admin b = Admin.create(properties(bootstrapB, (int) timeoutMillis));
            try {
                KafkaFuture<String> idA = a.describeCluster().clusterId();
                KafkaFuture<String> idB = b.describeCluster().clusterId();
                KafkaFuture.allOf(idA, idB).get(timeoutMillis, TimeUnit.MILLISECONDS);
                if (idA.get().equals(idB.get())) {
                    throw new IllegalArgumentException("A and B resolve to the same Kafka cluster");
                }
                return new Topology(idA.get(), idB.get());
            } finally {
                b.close(Duration.ofSeconds(1));
            }
        } finally {
            a.close(Duration.ofSeconds(1));
        }
    }

    private static Map<String, Object> properties(String bootstrapServers, int timeoutMillis) {
        return Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMillis,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMillis);
    }

    private static void requireBootstrap(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("bootstrap servers must not be blank");
        }
    }

    public record Topology(String clusterAId, String clusterBId) {}
}

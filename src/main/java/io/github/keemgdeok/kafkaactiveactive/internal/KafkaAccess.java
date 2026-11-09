package io.github.keemgdeok.kafkaactiveactive.internal;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import io.github.keemgdeok.kafkaactiveactive.replication.LogIdentity;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;

/** Native protocol helpers shared by replication and its acknowledgement readers. */
public final class KafkaAccess implements AutoCloseable {
    private final Admin admin;
    public KafkaAccess(Map<String, Object> properties, Duration timeout) {
        var values = new HashMap<>(properties);
        if (!values.containsKey("bootstrap.servers")) { throw new IllegalArgumentException("bootstrap.servers required"); }
        values.put("request.timeout.ms", (int) timeout.toMillis());
        values.put("default.api.timeout.ms", (int) timeout.toMillis());
        admin = Admin.create(values);
    }
    public LogIdentity identity(String topic, long deadline, boolean business) {
        var value = await(admin.describeTopics(List.of(topic)).allTopicNames(), deadline).get(topic);
        if (value.topicId().equals(Uuid.ZERO_UUID) || value.partitions().stream()
                .anyMatch(p -> p.leader() == null || p.leader().id() < 0)) {
            throw new org.apache.kafka.common.errors.TimeoutException("topic identity/leader not ready");
        }
        if (value.partitions().size() != 2
                || value.partitions().stream().anyMatch(p -> p.replicas().size() != 1)) {
            throw new IllegalArgumentException("expected native topic ID and two RF1 partitions");
        }
        if (business) {
            requireConfig(topic, Map.of("cleanup.policy", "delete", "retention.ms", "-1",
                    "retention.bytes", "-1"), deadline);
        }
        return new LogIdentity(await(admin.describeCluster().clusterId(), deadline), value.topicId().toString());
    }
    public void confirmationTopic(ReplicationConfig config, long deadline) {
        var values = Map.of("cleanup.policy", "delete", "retention.ms", "3600000", "retention.bytes", "-1");
        try {
            await(admin.createTopics(List.of(new NewTopic(config.confirmationTopic(), 2, (short) 1)
                    .configs(values))).all(), deadline);
        } catch (TopicExistsException exists) { /* Validate, never silently rewrite another configuration. */ }
        identity(config.confirmationTopic(), deadline, false);
        requireConfig(config.confirmationTopic(), values, deadline);
    }
    private void requireConfig(String topic, Map<String, String> expected, long deadline) {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        var actual = await(admin.describeConfigs(List.of(resource)).all(), deadline).get(resource);
        expected.forEach((key, value) -> {
            if (!value.equals(actual.get(key).value())) {
                throw new IllegalArgumentException(topic + " requires " + key + "=" + value);
            }
        });
    }
    public static long deadline(Duration duration) { return System.nanoTime() + duration.toNanos(); }
    public static <T> T await(Future<T> future, long deadline) {
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) { throw new org.apache.kafka.common.errors.TimeoutException("operation deadline elapsed"); }
            return future.get(remaining, TimeUnit.NANOSECONDS);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException runtime) { throw runtime; }
            throw new KafkaException("native operation failed", failed.getCause());
        } catch (java.util.concurrent.TimeoutException timedOut) {
            throw new org.apache.kafka.common.errors.TimeoutException("operation deadline elapsed", timedOut);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaException("native operation interrupted", interrupted);
        }
    }
    public static Map<String, Object> reader(Map<String, Object> supplied, Duration timeout,
                                             int batch, int maxEventBytes) {
        var config = new HashMap<>(supplied);
        if (config.containsKey("interceptor.classes")) { throw new IllegalArgumentException("interceptors unsupported"); }
        config.put("key.deserializer", ByteArrayDeserializer.class.getName());
        config.put("value.deserializer", ByteArrayDeserializer.class.getName());
        config.put("enable.auto.commit", false);
        config.put("auto.offset.reset", "none");
        config.put("isolation.level", "read_committed");
        config.put("allow.auto.create.topics", false);
        config.put("max.poll.records", batch);
        // A crashed/unreachable member must not delay each reconnect by the native 45s default.
        // Match the SDK application consumer defaults; callers can tune their broker's session bounds.
        config.putIfAbsent("session.timeout.ms", 6_000);
        config.putIfAbsent("heartbeat.interval.ms", 500);
        config.putIfAbsent("max.poll.interval.ms", 120_000);
        config.put("fetch.max.bytes", 16 * 1024 * 1024);
        config.put("max.partition.fetch.bytes", maxEventBytes + 1_024);
        config.put("default.api.timeout.ms", (int) timeout.toMillis());
        config.put("request.timeout.ms", (int) timeout.toMillis());
        return config;
    }
    public static Map<String, Object> writer(Map<String, Object> supplied, Duration timeout, int maxEventBytes) {
        var config = new HashMap<>(supplied);
        if (config.containsKey("interceptor.classes") || config.containsKey("transactional.id")
                || config.containsKey("partitioner.class")) {
            throw new IllegalArgumentException("external transactions, interceptors and partitioners unsupported");
        }
        config.put("key.serializer", ByteArraySerializer.class.getName());
        config.put("value.serializer", ByteArraySerializer.class.getName());
        config.put("acks", "all");
        config.put("enable.idempotence", true);
        config.put("linger.ms", 0);
        config.put("request.timeout.ms", 1_000);
        config.put("delivery.timeout.ms", (int) Math.max(1_000, timeout.toMillis()));
        config.put("max.block.ms", timeout.toMillis());
        config.put("buffer.memory", 32L * 1024 * 1024);
        config.put("max.request.size", maxEventBytes + 1_024);
        return config;
    }
    @Override public void close() { admin.close(Duration.ZERO); }
}

package io.github.keemgdeok.kafkaactiveactive.consume;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.keemgdeok.kafkaactiveactive.repair.RepairReport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** Kafka I/O for compacted completion topics. No native input consumer state is stored here. */
final class CompletionLedger implements AutoCloseable {
    private final ConsumerSettings settings;
    private final Map<String, Object> properties;
    private final Admin admin;
    private final KafkaConsumer<byte[], byte[]> reader;
    private KafkaProducer<byte[], byte[]> writer;

    CompletionLedger(ConsumerSettings settings, Map<String, Object> properties) {
        this.settings = settings;
        this.properties = Map.copyOf(properties);
        if (!properties.containsKey("bootstrap.servers") || properties.containsKey("interceptor.classes")
                || properties.containsKey("transactional.id")) {
            throw new IllegalArgumentException("bootstrap required; interceptors/transactional.id are SDK-owned");
        }
        admin = Admin.create(adminProperties(settings, properties));
        KafkaConsumer<byte[], byte[]> opened = null;
        try {
            opened = new KafkaConsumer<>(readerProperties(settings, properties));
            reader = opened;
            ensureTopic();
        } catch (RuntimeException failure) {
            try { if (opened != null) { opened.close(CloseOptions.timeout(Duration.ZERO)); } }
            finally { admin.close(Duration.ZERO); }
            throw failure;
        }
    }

    private void ensureTopic() {
        try {
            await(admin.createTopics(List.of(new NewTopic(settings.completionTopic(), 2, (short) 1).configs(Map.of(
                    "cleanup.policy", "compact", "retention.ms", "-1", "retention.bytes", "-1")))).all());
        } catch (IllegalStateException failure) {
            if (!(rootCause(failure) instanceof TopicExistsException)) { throw failure; }
        }
        snapshot();
    }

    Snapshot snapshot() {
        var description = await(admin.describeTopics(List.of(settings.completionTopic())).allTopicNames())
                .get(settings.completionTopic());
        if (description.partitions().size() != 2 || description.partitions().stream().anyMatch(p -> p.replicas().size() != 1)) {
            throw new IllegalStateException("completion topic must have two RF=1 partitions");
        }
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, settings.completionTopic());
        var config = await(admin.describeConfigs(List.of(resource)).all()).get(resource);
        if (!"compact".equals(config.get("cleanup.policy").value())
                || !"-1".equals(config.get("retention.ms").value()) || !"-1".equals(config.get("retention.bytes").value())) {
            throw new IllegalStateException("completion topic must retain compacted history without deletion");
        }
        var partitions = partitions();
        return new Snapshot(await(admin.describeCluster().clusterId()), description.topicId().toString(),
                reader.beginningOffsets(partitions, settings.operationTimeout()),
                reader.endOffsets(partitions, settings.operationTimeout()));
    }

    CompletedIndex replay(Snapshot boundary) {
        return replay(boundary, java.util.Set.of(0, 1));
    }

    CompletedIndex replay(Snapshot boundary, java.util.Set<Integer> owned) {
        var index = new CompletedIndex(settings);
        var assigned = partitions().stream().filter(partition -> owned.contains(partition.partition())).toList();
        reader.assign(assigned);
        assigned.forEach(partition -> reader.seek(partition, boundary.beginning().get(partition)));
        long deadline = System.nanoTime() + settings.operationTimeout().toNanos();
        while (assigned.stream().anyMatch(p -> reader.position(p, settings.operationTimeout()) < boundary.end().get(p))) {
            if (System.nanoTime() - deadline >= 0) { throw new IllegalStateException("completion replay deadline elapsed"); }
            for (var record : reader.poll(Duration.ofMillis(100))) {
                if (record.offset() < boundary.end().get(new TopicPartition(record.topic(), record.partition()))) {
                    index.add(CompletionMarker.decode(record, settings.maxEventBytes()));
                }
            }
        }
        var current = snapshot();
        verify(boundary, current);
        if (assigned.stream().anyMatch(partition -> !boundary.end().get(partition).equals(current.end().get(partition)))) {
            throw new IllegalStateException("completion history changed during replay; retry after writers are fenced/stopped");
        }
        return index;
    }

    void verify(Snapshot boundary) {
        verify(boundary, snapshot());
    }

    private static void verify(Snapshot boundary, Snapshot now) {
        if (!boundary.clusterId().equals(now.clusterId()) || !boundary.topicId().equals(now.topicId())
                || boundary.end().entrySet().stream().anyMatch(e -> now.end().get(e.getKey()) < e.getValue())) {
            throw new IllegalStateException("completion log identity or end changed");
        }
    }

    KafkaProducer<byte[], byte[]> transactionProducer(int partition) {
        var config = writerProperties(settings, properties);
        config.put("transactional.id", settings.transactionalId(partition));
        config.put("transaction.timeout.ms", 30_000);
        var producer = new KafkaProducer<byte[], byte[]>(config);
        try {
            producer.initTransactions();
            return producer;
        } catch (RuntimeException failure) {
            producer.close(Duration.ZERO);
            throw failure;
        }
    }

    void append(CompletionMarker marker) {
        if (writer == null) { writer = new KafkaProducer<>(writerProperties(settings, properties)); }
        await(writer.send(marker.record(settings.completionTopic())));
    }

    void requireGroupStopped() {
        try {
            var description = await(admin.describeConsumerGroups(List.of(settings.groupId())).all())
                    .get(settings.groupId());
            if (!description.members().isEmpty()) {
                throw new IllegalStateException("completion reconciliation requires the group stopped on both clusters");
            }
        } catch (IllegalStateException failure) {
            if (!(rootCause(failure) instanceof org.apache.kafka.common.errors.GroupIdNotFoundException)) { throw failure; }
        }
    }

    void fenceStoppedGroup() {
        requireGroupStopped();
        for (int partition = 0; partition < 2; partition++) {
            var producer = transactionProducer(partition);
            producer.close(Duration.ZERO);
        }
    }

    String businessTopicId() {
        var topic = await(admin.describeTopics(List.of(settings.topic())).allTopicNames()).get(settings.topic());
        if (topic.partitions().size() != 2 || topic.partitions().stream().anyMatch(p -> p.replicas().size() != 1)) {
            throw new IllegalStateException("business topic must have two RF=1 partitions");
        }
        return topic.topicId().toString();
    }

    void resetInputOffsets(Snapshot boundary) {
        requireGroupStopped();
        verify(boundary);
        String metadata = "kaa-ledger=" + boundary.topicId() + ";input=" + businessTopicId();
        await(admin.alterConsumerGroupOffsets(settings.groupId(), Map.of(
                new TopicPartition(settings.topic(), 0), new OffsetAndMetadata(0, metadata),
                new TopicPartition(settings.topic(), 1), new OffsetAndMetadata(0, metadata))).all());
    }

    void verifyBusinessBoundary(RepairReport.LogBoundary expected) {
        if (!expected.topicId().equals(businessTopicId())
                || !expected.clusterId().equals(await(admin.describeCluster().clusterId()))) {
            throw new IllegalStateException("business log identity changed after preparation");
        }
        var p0 = new TopicPartition(settings.topic(), 0);
        var p1 = new TopicPartition(settings.topic(), 1);
        var options = new ListOffsetsOptions(IsolationLevel.READ_COMMITTED);
        var beginnings = await(admin.listOffsets(Map.of(p0, OffsetSpec.earliest(),
                p1, OffsetSpec.earliest()), options).all());
        var ends = await(admin.listOffsets(Map.of(p0, OffsetSpec.latest(),
                p1, OffsetSpec.latest()), options).all());
        for (var partition : List.of(p0, p1)) {
            var before = expected.partitions().get(partition.partition());
            var after = ends.get(partition);
            if (beginnings.get(partition).offset() != before.beginning() || after.offset() < before.end()
                    || after.leaderEpoch().isEmpty() || after.leaderEpoch().orElseThrow() != before.leaderEpoch()) {
                throw new IllegalStateException("business log boundary or epoch changed after preparation");
            }
        }
    }

    static Map<String, Object> readerProperties(ConsumerSettings settings, Map<String, Object> nativeConfig) {
        var config = adminProperties(settings, nativeConfig);
        config.put("key.deserializer", ByteArrayDeserializer.class.getName());
        config.put("value.deserializer", ByteArrayDeserializer.class.getName());
        config.put("enable.auto.commit", false);
        config.put("auto.offset.reset", "earliest");
        config.put("isolation.level", "read_committed");
        config.put("allow.auto.create.topics", false);
        config.put("max.poll.records", settings.maxBatchRecords());
        config.put("fetch.max.bytes", 16 * 1024 * 1024);
        config.put("max.partition.fetch.bytes", settings.maxEventBytes() + 1_024);
        return config;
    }

    static Map<String, Object> adminProperties(ConsumerSettings settings, Map<String, Object> nativeConfig) {
        var config = new HashMap<String, Object>(nativeConfig);
        config.put("request.timeout.ms", 1_000);
        config.put("default.api.timeout.ms", (int) settings.operationTimeout().toMillis());
        return config;
    }

    private static Map<String, Object> writerProperties(ConsumerSettings settings, Map<String, Object> nativeConfig) {
        var config = adminProperties(settings, nativeConfig);
        config.put("key.serializer", ByteArraySerializer.class.getName());
        config.put("value.serializer", ByteArraySerializer.class.getName());
        config.put("acks", "all");
        config.put("enable.idempotence", true);
        config.put("max.in.flight.requests.per.connection", 1);
        config.put("retries", Integer.MAX_VALUE);
        config.put("max.block.ms", settings.operationTimeout().toMillis());
        config.put("linger.ms", 0);
        config.put("delivery.timeout.ms", (int) settings.operationTimeout().toMillis());
        config.put("buffer.memory", 32L * 1024 * 1024);
        config.put("max.request.size", settings.maxEventBytes() + 1_024);
        return config;
    }

    private <T> T await(Future<T> future) {
        try { return future.get(settings.operationTimeout().toNanos(), TimeUnit.NANOSECONDS); }
        catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("completion operation interrupted", failure);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("completion operation failed", failure);
        }
    }

    private List<TopicPartition> partitions() {
        return List.of(new TopicPartition(settings.completionTopic(), 0), new TopicPartition(settings.completionTopic(), 1));
    }
    private static Throwable rootCause(Throwable failure) {
        while (failure.getCause() != null) { failure = failure.getCause(); }
        return failure;
    }

    @Override public void close() {
        try { if (writer != null) { writer.close(Duration.ZERO); } }
        finally {
            try { reader.close(CloseOptions.timeout(Duration.ZERO)); }
            finally { admin.close(Duration.ZERO); }
        }
    }
    record Snapshot(String clusterId, String topicId, Map<TopicPartition, Long> beginning, Map<TopicPartition, Long> end) {}
}

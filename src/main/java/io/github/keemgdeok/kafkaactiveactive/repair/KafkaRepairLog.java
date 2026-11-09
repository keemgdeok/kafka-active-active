package io.github.keemgdeok.kafkaactiveactive.repair;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

final class KafkaRepairLog implements RepairLog {
    private final RepairConfig config;
    private final Deadline deadline;
    private final Admin admin;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProducer<byte[], byte[]> producer;
    private final List<TopicPartition> partitions;

    KafkaRepairLog(RepairConfig config, Map<String, Object> properties, Deadline deadline) {
        this.config = config;
        this.deadline = deadline;
        Map<String, Object> adminProperties = new HashMap<>(properties);
        adminProperties.put("request.timeout.ms", 1_000);
        adminProperties.put("default.api.timeout.ms", 5_000);
        admin = Admin.create(adminProperties);
        KafkaConsumer<byte[], byte[]> reader = null;
        try {
            Map<String, Object> consumerProperties = new HashMap<>(properties);
            consumerProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
            consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
            consumerProperties.put("enable.auto.commit", false);
            consumerProperties.put("auto.offset.reset", "none");
            consumerProperties.put("isolation.level", "read_committed");
            consumerProperties.put("allow.auto.create.topics", false);
            consumerProperties.put("max.poll.records", 100);
            consumerProperties.put("fetch.max.bytes", 16 * 1024 * 1024);
            consumerProperties.put("max.partition.fetch.bytes", config.maxEventBytes() + 1_024);
            consumerProperties.put("default.api.timeout.ms", 5_000);
            reader = new KafkaConsumer<>(consumerProperties);
            consumer = reader;
            Map<String, Object> producerProperties = new HashMap<>(properties);
            producerProperties.put("key.serializer", ByteArraySerializer.class.getName());
            producerProperties.put("value.serializer", ByteArraySerializer.class.getName());
            producerProperties.put("acks", "all");
            producerProperties.put("enable.idempotence", true);
            producerProperties.put("max.in.flight.requests.per.connection", 1);
            producerProperties.put("retries", Integer.MAX_VALUE);
            producerProperties.put("linger.ms", 0);
            producerProperties.put("max.block.ms", Math.min(1_000L, config.runTimeout().toMillis()));
            producerProperties.put("request.timeout.ms", 1_000);
            producerProperties.put("delivery.timeout.ms", 5_000);
            producerProperties.put("buffer.memory", 32L * 1024 * 1024);
            producerProperties.put("max.request.size", config.maxEventBytes() + 1_024);
            producer = new KafkaProducer<>(producerProperties);
            partitions = List.of(new TopicPartition(config.topic(), 0), new TopicPartition(config.topic(), 1));
        } catch (RuntimeException failure) {
            if (reader != null) {
                reader.close(CloseOptions.timeout(Duration.ZERO));
            }
            admin.close(Duration.ZERO);
            throw failure;
        }
    }

    @Override
    public RepairReport.LogBoundary capture() {
        deadline.remaining();
        var topic = await(admin.describeTopics(List.of(config.topic())).allTopicNames()).get(config.topic());
        if (topic.topicId().equals(Uuid.ZERO_UUID) || topic.partitions().size() != 2
                || topic.partitions().stream().anyMatch(p -> p.replicas().size() != 1)) {
            throw new RepairException(RepairException.Reason.INVALID_TOPOLOGY, "expected topic ID and two RF=1 partitions");
        }
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, config.topic());
        var topicConfig = await(admin.describeConfigs(List.of(resource)).all()).get(resource);
        if (!"-1".equals(topicConfig.get("retention.ms").value())
                || !"-1".equals(topicConfig.get("retention.bytes").value())
                || !"delete".equals(topicConfig.get("cleanup.policy").value())) {
            throw new RepairException(RepairException.Reason.INVALID_TOPOLOGY,
                    "repair requires cleanup.policy=delete and retention.ms/bytes=-1");
        }
        String clusterId = await(admin.describeCluster().clusterId());
        var options = new ListOffsetsOptions(IsolationLevel.READ_COMMITTED);
        var beginning = await(admin.listOffsets(Map.of(partitions.get(0), OffsetSpec.earliest(),
                partitions.get(1), OffsetSpec.earliest()), options).all());
        var ends = await(admin.listOffsets(Map.of(partitions.get(0), OffsetSpec.latest(),
                partitions.get(1), OffsetSpec.latest()), options).all());
        Map<Integer, RepairReport.PartitionBoundary> bounds = new HashMap<>();
        for (TopicPartition partition : partitions) {
            var end = ends.get(partition);
            if (beginning.get(partition).offset() != 0 || end.leaderEpoch().isEmpty()) {
                throw new RepairException(RepairException.Reason.LOG_CHANGED,
                        "retained log beginning or leader epoch is unavailable");
            }
            bounds.put(partition.partition(), new RepairReport.PartitionBoundary(0, end.offset(),
                    end.leaderEpoch().orElseThrow()));
        }
        return new RepairReport.LogBoundary(clusterId, topic.topicId().toString(), bounds);
    }

    @Override
    public void scan(RepairReport.LogBoundary boundary, Map<Integer, Long> from,
                     Consumer<ConsumerRecord<byte[], byte[]>> visitor) {
        verify(boundary);
        consumer.assign(partitions);
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, from.get(partition.partition()));
        }
        while (true) {
            deadline.remaining();
            boolean complete = true;
            for (TopicPartition partition : partitions) {
                if (consumer.position(partition, deadline.operationTimeout())
                        < boundary.partitions().get(partition.partition()).end()) {
                    complete = false;
                    break;
                }
            }
            if (complete) {
                return;
            }
            Duration wait = deadline.operationTimeout();
            if (wait.compareTo(Duration.ofMillis(200)) > 0) {
                wait = Duration.ofMillis(200);
            }
            for (var record : consumer.poll(wait)) {
                var bounds = boundary.partitions().get(record.partition());
                if (record.offset() >= from.get(record.partition()) && record.offset() < bounds.end()) {
                    visitor.accept(record);
                }
            }
        }
    }

    @Override
    public void append(ConsumerRecord<byte[], byte[]> record) {
        deadline.remaining();
        var copy = io.github.keemgdeok.kafkaactiveactive.internal.EventCodec.replica(
                config.topic(), record, config.maxEventBytes());
        var ack = await(producer.send(copy));
        if (!ack.hasOffset()) {
            throw new RepairException(RepairException.Reason.CLIENT_FAILURE, "repair append returned no offset");
        }
    }

    private <T> T await(Future<T> future) {
        try {
            return future.get(deadline.operationTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new RepairException(RepairException.Reason.CLOSED, "repair interrupted", failure);
        } catch (ExecutionException | TimeoutException failure) {
            deadline.remaining();
            throw new RepairException(RepairException.Reason.CLIENT_FAILURE, "Kafka repair request failed", failure);
        }
    }

    @Override
    public void close() {
        try {
            producer.close(Duration.ZERO);
        } finally {
            try {
                consumer.close(CloseOptions.timeout(Duration.ZERO));
            } finally {
                admin.close(Duration.ZERO);
            }
        }
    }
}

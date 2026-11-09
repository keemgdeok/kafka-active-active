package io.github.keemgdeok.kafkaactiveactive.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess;

/** Native finite-log evidence independent of SDK counters and response grades. */
public final class NativeLog {
    private NativeLog() {}
    public static List<ConsumerRecord<byte[], byte[]>> read(Map<String, Object> properties, String topic) {
        var reader = new KafkaConsumer<byte[], byte[]>(KafkaAccess.reader(properties, Duration.ofSeconds(5), 100, 900_000));
        try {
            var partitions = List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1));
            reader.assign(partitions); reader.seekToBeginning(partitions);
            var end = reader.endOffsets(partitions, Duration.ofSeconds(5));
            var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (partitions.stream().anyMatch(p -> reader.position(p) < end.get(p))) {
                if (System.nanoTime() >= deadline) { throw new AssertionError("native finite boundary not reached: " + end); }
                for (var record : reader.poll(Duration.ofMillis(100))) {
                    if (record.offset() < end.get(new TopicPartition(topic, record.partition()))) { records.add(record); }
                }
            }
            return List.copyOf(records);
        } finally { reader.close(CloseOptions.timeout(Duration.ZERO)); }
    }
    public static void write(Map<String, Object> properties, ProducerRecord<byte[], byte[]> record) throws Exception {
        var writer = new KafkaProducer<byte[], byte[]>(KafkaAccess.writer(properties, Duration.ofSeconds(5), 900_000));
        try { writer.send(record).get(10, TimeUnit.SECONDS); }
        finally { writer.close(Duration.ZERO); }
    }
}

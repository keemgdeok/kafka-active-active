package io.github.keemgdeok.kafkaactiveactive.verification;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Reads real logs to finite boundaries; topic unions support the default MM2 naming policy. */
public final class EventAudit {
    private EventAudit() {}
    public static Report read(Map<String, Object> nativeProperties, Collection<String> topics, List<Event> expected) {
        var properties = new HashMap<String, Object>(nativeProperties);
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", false);
        properties.put("isolation.level", "read_committed");
        properties.put("auto.offset.reset", "none");
        properties.put("default.api.timeout.ms", 10_000);
        Map<String, Event> byId = new HashMap<>();
        Map<String, Integer> sequence = new HashMap<>();
        for (int i = 0; i < expected.size(); i++) {
            Event old = byId.put(expected.get(i).eventId(), expected.get(i));
            if (old != null) { throw new IllegalArgumentException("expected IDs must be unique"); }
            sequence.put(expected.get(i).eventId(), i);
        }
        Set<String> seen = new HashSet<>();
        Set<String> unexpected = new HashSet<>();
        Map<String, Integer> previous = new HashMap<>();
        long physical = 0;
        long conflicts = 0;
        long inversions = 0;
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            var partitions = topics.stream().flatMap(topic -> List.of(new TopicPartition(topic, 0), new TopicPartition(topic, 1)).stream()).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var ends = consumer.endOffsets(partitions, Duration.ofSeconds(10));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                if (System.nanoTime() - deadline >= 0) { throw new IllegalStateException("audit did not reach finite log boundary"); }
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (record.offset() >= ends.get(new TopicPartition(record.topic(), record.partition()))) { continue; }
                    physical++;
                    var identity = EventCodec.identity(record, 16 * 1024 * 1024);
                    Event event = byId.get(identity.id());
                    if (event == null) { unexpected.add(identity.id()); continue; }
                    if (!Arrays.equals(event.payload(), record.value()) || !Arrays.equals(event.key().getBytes(StandardCharsets.UTF_8), record.key())
                            || event.partition() != record.partition()) { conflicts++; }
                    if (seen.add(identity.id())) {
                        String key = record.topic() + ":" + event.key();
                        int index = sequence.get(identity.id());
                        Integer old = previous.put(key, index);
                        if (old != null && old > index) { inversions++; }
                    }
                }
            }
        }
        var missing = new HashSet<>(byId.keySet());
        missing.removeAll(seen);
        return new Report(physical, seen.size(), Set.copyOf(missing), Set.copyOf(unexpected), conflicts, inversions);
    }
    public record Report(long physicalRecords, int uniqueEvents, Set<String> missingIds,
                          Set<String> unexpectedIds, long payloadConflicts, long keyOrderInversions) {
        public void requireComplete() {
            if (!missingIds.isEmpty() || !unexpectedIds.isEmpty() || payloadConflicts != 0) {
                throw new IllegalStateException("event audit failed: " + this);
            }
        }
    }
}

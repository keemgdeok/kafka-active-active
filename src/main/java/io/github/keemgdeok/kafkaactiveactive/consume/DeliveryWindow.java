package io.github.keemgdeok.kafkaactiveactive.consume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Pure batch selection and prefix-offset calculation, independent of Kafka I/O. */
final class DeliveryWindow {
    private final List<ConsumerRecord<byte[], byte[]>> raw;
    private final Map<String, CompletionMarker> identities = new LinkedHashMap<>();
    private final List<CompletionMarker> rawIdentities = new ArrayList<>();
    private final Map<Integer, Long> ends;
    private final CompletedIndex completed;
    private final Batch batch;

    DeliveryWindow(List<ConsumerRecord<byte[], byte[]>> raw, Map<Integer, Long> ends,
                   CompletedIndex completed, int maxEventBytes) {
        this.raw = List.copyOf(raw);
        this.ends = Map.copyOf(ends);
        this.completed = completed;
        List<Batch.Delivery> deliveries = new ArrayList<>();
        Map<Integer, Long> previous = new HashMap<>();
        for (var record : raw) {
            Long oldOffset = previous.put(record.partition(), record.offset());
            if (oldOffset != null && record.offset() <= oldOffset) {
                throw new IllegalStateException("unordered input partition");
            }
            var marker = CompletionMarker.event(record, maxEventBytes);
            rawIdentities.add(marker);
            var old = identities.putIfAbsent(marker.id(), marker);
            if (old != null && !old.matches(marker)) { throw new IllegalStateException("batch ID content conflict"); }
            if (!completed.contains(marker) && old == null) {
                deliveries.add(new Batch.Delivery(EventCodec.event(record, maxEventBytes),
                        record.partition(), record.offset()));
            }
        }
        batch = new Batch(deliveries);
    }

    Batch batch() { return batch; }
    long filtered() { return raw.size() - batch.records().size(); }

    Plan plan(ProcessedBatch processed) {
        if (!batch.owns(processed)) { throw new IllegalArgumentException("batch belongs to another poll"); }
        Map<String, CompletionMarker> selected = new HashMap<>();
        for (var delivery : batch.records().subList(0, processed.count())) {
            selected.put(delivery.event().eventId(), identities.get(delivery.event().eventId()));
        }
        completed.reserve(selected.values());
        Map<Integer, Long> next = new HashMap<>();
        raw.forEach(record -> next.put(record.partition(), ends.get(record.partition())));
        Map<Integer, Long> rewind = new HashMap<>();
        for (int i = 0; i < raw.size(); i++) {
            var record = raw.get(i);
            if (rewind.containsKey(record.partition())) { continue; }
            var marker = rawIdentities.get(i);
            if (!completed.contains(marker) && !selected.containsKey(marker.id())) {
                next.put(record.partition(), record.offset());
                rewind.put(record.partition(), record.offset());
            }
        }
        Map<Integer, List<CompletionMarker>> markers = new HashMap<>();
        selected.values().forEach(marker -> markers.computeIfAbsent(marker.partition(), ignored -> new ArrayList<>()).add(marker));
        return new Plan(Map.copyOf(next), Map.copyOf(rewind), markers);
    }

    Map<Integer, Long> beginnings() {
        Map<Integer, Long> starts = new HashMap<>();
        raw.forEach(record -> starts.merge(record.partition(), record.offset(), Math::min));
        return starts;
    }

    record Plan(Map<Integer, Long> next, Map<Integer, Long> rewind, Map<Integer, List<CompletionMarker>> markers) {}
}

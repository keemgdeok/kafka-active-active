package io.github.keemgdeok.kafkaactiveactive.baseline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import io.github.keemgdeok.kafkaactiveactive.environment.KafkaTopologyVerifier;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.publish.DualKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishResult;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairService;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.EventDataset;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** Correctness comparison, not a throughput/p99 benchmark or an external-sink exactly-once claim. */
class MirrorMakerComparisonTest {
    @Test void sameInputsSurviveOutageAndMm2TranslatesAnInactiveGroupsOriginOffsets() throws Exception {
        int count = Integer.parseInt(System.getProperty("kaa.baseline.count", "24"));
        long seed = Long.parseLong(System.getProperty("kaa.baseline.seed", "0"));
        if (count < 12 || count > 1_000) { throw new IllegalArgumentException("baselineCount must be 12..1000"); }
        var events = EventDataset.generate(count, seed, false);
        String sdkTopic = "sdk-baseline-" + UUID.randomUUID();
        String mirrorTopic = "mm2-baseline-" + UUID.randomUUID();
        try (var environment = new MirrorEnvironment();
             var evidence = new JsonlEvidence(Path.of("build/reports/baseline/comparison.jsonl"))) {
            environment.start();
            var topology = KafkaTopologyVerifier.verify(environment.a.getBootstrapServers(), environment.b.getBootstrapServers(), Duration.ofSeconds(10));
            evidence.record("scenario", Map.of("seed", seed, "events", count, "cluster_a", topology.clusterAId(),
                    "cluster_b", topology.clusterBId(), "kafka_version", "4.3.2", "fault", "PAUSE_A", "outage_inputs", 4));
            environment.createTopic(sdkTopic);
            var sdk = sdk(environment, sdkTopic, events, evidence);
            environment.createTopic(mirrorTopic);
            environment.startMirror(mirrorTopic);
            var mirror = mirror(environment, mirrorTopic, events, evidence);
            String header = "implementation\tinput_events\tunique_events_per_kafka\tphysical_a\tphysical_b\tcomplete_after_resume_ms\tsubscription_a\tsubscription_b\n";
            Files.writeString(Path.of("build/reports/baseline/comparison.tsv"), header
                    + row("sdk", count, sdk, sdkTopic, sdkTopic)
                    + row("mm2", count, mirror, mirrorTopic + " + B." + mirrorTopic, mirrorTopic + " + A." + mirrorTopic));
            evidence.record("verified", Map.of("seed", seed, "events_per_kafka", count, "sdk_physical_a", sdk.physicalA(),
                    "sdk_physical_b", sdk.physicalB(), "mm2_physical_a", mirror.physicalA(), "mm2_physical_b", mirror.physicalB(),
                    "missing", 0, "payload_conflicts", 0, "native_checkpoint_translation", true));
        }
    }

    private static Result sdk(MirrorEnvironment environment, String topic, List<Event> events, JsonlEvidence evidence) throws Exception {
        int warm = events.size() / 3;
        long resumed;
        try (var publisher = DualKafkaPublisher.open(PublishConfig.defaults(topic), environment.aProperties(), environment.bProperties())) {
            for (var event : events.subList(0, warm)) {
                assertEquals(PublishResult.Status.CONFIRMED, sdkSend(publisher, event, evidence).status());
            }
            environment.pauseA();
            try {
                var pending = events.subList(warm, warm + 4).stream().map(publisher::send).toList();
                for (var stage : pending) {
                    var result = stage.toCompletableFuture().get(10, TimeUnit.SECONDS);
                    recordSdk(result, evidence);
                    assertEquals(PublishResult.Status.ACCEPTED_DEGRADED, result.status());
                }
            } finally { resumed = System.nanoTime(); environment.resumeA(); }
            for (var event : events.subList(warm + 4, events.size())) {
                assertTrue(sdkSend(publisher, event, evidence).successful());
            }
        }
        try (var repair = new RepairService(RepairConfig.defaults(topic), environment.aProperties(), environment.bProperties())) {
            var report = repair.runOnce();
            evidence.record("sdk_repair", Map.of("run_id", report.runId(), "unique_indexed", report.uniqueEvents(),
                    "copied_to_a", report.copiedToA(), "copied_to_b", report.copiedToB()));
            var repeated = repair.runOnce(); assertEquals(0, repeated.copiedToA() + repeated.copiedToB());
        }
        var a = EventAudit.read(environment.aProperties(), List.of(topic), events);
        var b = EventAudit.read(environment.bProperties(), List.of(topic), events);
        a.requireComplete(); b.requireComplete();
        return result("sdk", a, b, resumed, evidence);
    }

    private static Result mirror(MirrorEnvironment environment, String topic, List<Event> events, JsonlEvidence evidence) throws Exception {
        int warm = events.size() / 3;
        long resumed;
        var originsA = new ArrayList<Event>();
        try (var a = producer(environment.aProperties()); var b = producer(environment.bProperties())) {
            routeOriginals(events.subList(0, warm), a, b, topic, originsA, evidence);
            awaitMirrors(environment, topic, events.subList(0, warm));
            environment.pauseA();
            try {
                // This fallback is explicit application routing, not a feature supplied by MM2.
                for (var event : events.subList(warm, warm + 4)) { origin(event, b, topic, "B-fallback", evidence); }
            } finally { resumed = System.nanoTime(); environment.resumeA(); }
            routeOriginals(events.subList(warm + 4, events.size()), a, b, topic, originsA, evidence);
        }
        var reports = awaitMirrors(environment, topic, events);
        var result = result("mm2", reports.get(0), reports.get(1), resumed, evidence);
        assertFalse(originsA.isEmpty(), "comparison must exercise A-origin replication");
        verifyCheckpoint(environment, topic, originsA, evidence);
        for (var properties : List.of(environment.aProperties(), environment.bProperties())) {
            try (var admin = Admin.create(properties)) {
                var names = admin.listTopics().names().get(5, TimeUnit.SECONDS);
                assertTrue(names.stream().filter(name -> name.endsWith("." + topic)).allMatch(name -> name.equals("A." + topic) || name.equals("B." + topic)),
                        "unexpected recursively prefixed topic: " + names);
            }
        }
        return result;
    }

    private static List<EventAudit.Report> awaitMirrors(MirrorEnvironment environment, String topic, List<Event> events) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        List<EventAudit.Report> latest = List.of();
        try (var a = Admin.create(environment.aProperties()); var b = Admin.create(environment.bProperties())) {
            while (System.nanoTime() < deadline) {
                if (a.listTopics().names().get(5, TimeUnit.SECONDS).contains("B." + topic)
                        && b.listTopics().names().get(5, TimeUnit.SECONDS).contains("A." + topic)) {
                    latest = List.of(EventAudit.read(environment.aProperties(), List.of(topic, "B." + topic), events),
                            EventAudit.read(environment.bProperties(), List.of(topic, "A." + topic), events));
                    for (var report : latest) { assertEquals(0, report.payloadConflicts()); assertTrue(report.unexpectedIds().isEmpty()); }
                    if (latest.stream().allMatch(report -> report.missingIds().isEmpty())) { return latest; }
                }
                LockSupport.parkNanos(Duration.ofMillis(200).toNanos());
            }
        }
        throw new AssertionError("MM2 did not converge to the expected local+remote event union: " + latest);
    }

    private static void verifyCheckpoint(MirrorEnvironment environment, String topic, List<Event> expected, JsonlEvidence evidence) throws Exception {
        String group = "baseline-group-" + UUID.randomUUID();
        var properties = new HashMap<String, Object>(environment.aProperties());
        properties.put("group.id", group); properties.put("group.protocol", "classic");
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", false); properties.put("auto.offset.reset", "earliest");
        Set<Event> seen = new HashSet<>();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (seen.size() < expected.size() && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) { seen.add(EventCodec.event(record, 1_024)); }
            }
            assertEquals(Set.copyOf(expected), seen);
            consumer.commitSync(Duration.ofSeconds(5));
        } // Native source group is inactive before MM2 syncs its translated offsets into B.
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        try (var target = Admin.create(environment.bProperties())) {
            while (System.nanoTime() < deadline) {
                try {
                    var offsets = target.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                    if (offsets.entrySet().stream().anyMatch(entry -> entry.getKey().topic().equals("A." + topic) && entry.getValue().offset() > 0)) {
                        assertTrue(offsets.values().stream().allMatch(offset -> offset.offset() >= 0));
                        Map<TopicPartition, OffsetSpec> queries = new HashMap<>();
                        offsets.keySet().forEach(partition -> queries.put(partition, OffsetSpec.latest()));
                        var ends = target.listOffsets(queries).all().get(5, TimeUnit.SECONDS);
                        offsets.forEach((partition, offset) -> assertTrue(offset.offset() <= ends.get(partition).offset(), "translated offset exceeds the native target log"));
                        assertTrue(target.describeConsumerGroups(List.of(group)).all().get(5, TimeUnit.SECONDS).get(group).members().isEmpty());
                        evidence.record("mm2_checkpoint", Map.of("group", group, "source_topic", topic, "target_topic", "A." + topic,
                                "translated_offsets", offsets, "source_records_consumed", expected.size(), "target_group_active", false));
                        return;
                    }
                } catch (java.util.concurrent.ExecutionException failure) {
                    if (!(failure.getCause() instanceof GroupIdNotFoundException)) { throw failure; }
                }
                LockSupport.parkNanos(Duration.ofMillis(200).toNanos());
            }
        }
        throw new AssertionError("MM2 did not publish native translated offsets for the inactive origin group");
    }

    private static KafkaProducer<byte[], byte[]> producer(Map<String, Object> nativeProperties) {
        var properties = new HashMap<String, Object>(nativeProperties);
        properties.put("key.serializer", ByteArraySerializer.class.getName()); properties.put("value.serializer", ByteArraySerializer.class.getName());
        properties.put("acks", "all"); properties.put("enable.idempotence", true);
        properties.put("request.timeout.ms", 1_000); properties.put("delivery.timeout.ms", 5_000); properties.put("max.block.ms", 5_000);
        return new KafkaProducer<>(properties);
    }
    private static void origin(Event event, KafkaProducer<byte[], byte[]> producer, String topic, String origin, JsonlEvidence evidence) throws Exception {
        var metadata = producer.send(EventCodec.encode(topic, event)).get(8, TimeUnit.SECONDS);
        evidence.record("mm2_origin_ack", Map.of("event_id", event.eventId(), "origin", origin, "partition", metadata.partition(), "offset", metadata.offset()));
    }
    private static void routeOriginals(List<Event> events, KafkaProducer<byte[], byte[]> a, KafkaProducer<byte[], byte[]> b,
                                       String topic, List<Event> originsA, JsonlEvidence evidence) throws Exception {
        for (var event : events) {
            boolean ownedByA = event.partition() == 0;
            origin(event, ownedByA ? a : b, topic, ownedByA ? "A" : "B", evidence);
            if (ownedByA) { originsA.add(event); }
        }
    }
    private static PublishResult sdkSend(DualKafkaPublisher publisher, Event event, JsonlEvidence evidence) throws Exception {
        var result = publisher.send(event).toCompletableFuture().get(10, TimeUnit.SECONDS); recordSdk(result, evidence); return result;
    }
    private static void recordSdk(PublishResult result, JsonlEvidence evidence) {
        evidence.record("sdk_ack", Map.of("event_id", result.eventId(), "status", result.status(), "a_delivery", result.clusterA().delivery(), "b_delivery", result.clusterB().delivery()));
    }
    private static Result result(String implementation, EventAudit.Report a, EventAudit.Report b, long resumed, JsonlEvidence evidence) {
        var result = new Result((System.nanoTime() - resumed) / 1_000_000, a.physicalRecords(), b.physicalRecords());
        evidence.record("event_audit", Map.of("implementation", implementation, "unique_a", a.uniqueEvents(), "unique_b", b.uniqueEvents(),
                "physical_a", a.physicalRecords(), "physical_b", b.physicalRecords(), "complete_after_resume_ms", result.completeAfterResumeMillis(),
                "key_order_inversions_a", a.keyOrderInversions(), "key_order_inversions_b", b.keyOrderInversions()));
        return result;
    }
    private static String row(String implementation, int count, Result result, String subscriptionA, String subscriptionB) {
        return implementation + "\t" + count + "\t" + count + "\t" + result.physicalA() + "\t" + result.physicalB() + "\t"
                + result.completeAfterResumeMillis() + "\t" + subscriptionA + "\t" + subscriptionB + "\n";
    }
    private record Result(long completeAfterResumeMillis, long physicalA, long physicalB) {}
}

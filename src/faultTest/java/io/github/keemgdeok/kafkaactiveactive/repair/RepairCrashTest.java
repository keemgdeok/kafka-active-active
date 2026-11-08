package io.github.keemgdeok.kafkaactiveactive.repair;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.support.CrashProcess;
import io.github.keemgdeok.kafkaactiveactive.support.FaultSeeds;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.EventDataset;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class RepairCrashTest {
    private static final KafkaPair KAFKA = new KafkaPair("fault-repair-brokers");
    @BeforeAll static void start() { KAFKA.start(); }
    @AfterAll static void stop() throws Exception { KAFKA.close(); }
    static IntStream seeds() { return FaultSeeds.values(); }
    @ParameterizedTest(name = "seed={0} repeated repair crash") @MethodSource("seeds")
    void repeatedCrashesAfterTargetAckRebuildStateWithoutUnboundedCopies(int seed) throws Exception {
        String topic = "repair-crash-" + UUID.randomUUID();
        KAFKA.createTopic(topic);
        var events = EventDataset.generate(8 + seed % 8, seed, seed % 2 == 0);
        var properties = new java.util.HashMap<String, Object>(KAFKA.aProperties());
        properties.put("key.serializer", ByteArraySerializer.class.getName());
        properties.put("value.serializer", ByteArraySerializer.class.getName());
        properties.put("acks", "all");
        try (var producer = new KafkaProducer<byte[], byte[]>(properties)) {
            for (var event : events) { producer.send(EventCodec.encode(topic, event)).get(10, TimeUnit.SECONDS); }
        }
        try (var evidence = new JsonlEvidence(Path.of("build/reports/faultTest", "repair-" + seed + ".jsonl"))) {
            evidence.record("scenario", Map.of("seed", seed, "topic", topic, "events", events.size(),
                    "hot_key", seed % 2 == 0, "fault_point", "REPAIR_TARGET_ACK", "crashes", 2));
            for (int iteration = 0; iteration < 2; iteration++) {
                CrashProcess.run(RepairCrashChild.class.getName(), "repair-" + seed + "-" + iteration,
                        topic, KAFKA.a.getBootstrapServers(), KAFKA.b.getBootstrapServers());
                assertEquals(iteration + 1, EventAudit.read(KAFKA.bProperties(), List.of(topic), events).physicalRecords());
                evidence.record("crash", Map.of("seed", seed, "iteration", iteration, "fault_point", "REPAIR_TARGET_ACK", "acked_copies", iteration + 1));
            }
            try (var repair = new RepairService(RepairConfig.defaults(topic), KAFKA.aProperties(), KAFKA.bProperties())) {
                assertEquals(events.size() - 2, repair.runOnce().copiedToB());
                for (int i = 0; i < 3; i++) {
                    var report = repair.runOnce(); assertEquals(0, report.copiedToA() + report.copiedToB());
                }
            }
            var a = EventAudit.read(KAFKA.aProperties(), List.of(topic), events);
            var b = EventAudit.read(KAFKA.bProperties(), List.of(topic), events);
            a.requireComplete(); b.requireComplete();
            assertEquals(events.size(), b.physicalRecords());
            evidence.record("verified", Map.of("seed", seed, "unique_events", events.size(), "physical_records_a", a.physicalRecords(),
                    "physical_records_b", b.physicalRecords(), "missing", 0, "payload_conflicts", 0, "repeated_repair_growth", 0));
        }
    }
}

package io.github.keemgdeok.kafkaactiveactive.replication;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess;
import io.github.keemgdeok.kafkaactiveactive.support.CrashProcess;
import io.github.keemgdeok.kafkaactiveactive.support.FaultSeeds;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.NativeLog;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

class ReplicationCrashTest {
    private static final KafkaPair PAIR = new KafkaPair("fault-streaming-brokers");
    @BeforeAll static void start() { PAIR.start(); }
    @AfterAll static void stop() throws Exception { PAIR.close(); }
    static Stream<Arguments> cases() {
        return FaultSeeds.values().boxed().flatMap(seed -> Stream.of("BEFORE_TARGET_APPEND", "AFTER_TARGET_ACK",
                "BEFORE_SOURCE_COMMIT", "AFTER_SOURCE_COMMIT").map(stage -> Arguments.of(seed, stage)));
    }
    @ParameterizedTest(name="seed={0} replication-halt={1}") @MethodSource("cases")
    void targetAckAndSourceTransactionBoundariesSurviveActualProcessExit(int seed, String stage) throws Exception {
        String topic = "repl-halt-" + UUID.randomUUID(); PAIR.createTopic(topic);
        var event = ReplicationCrashChild.input(seed); var attempt = UUID.randomUUID();
        var source = seed % 2 == 0 ? PAIR.aProperties() : PAIR.bProperties();
        var target = seed % 2 == 0 ? PAIR.bProperties() : PAIR.aProperties();
        var config = ReplicationConfig.defaults(topic);
        LogIdentity from; LogIdentity to;
        try (var a = new KafkaAccess(source, Duration.ofSeconds(5)); var b = new KafkaAccess(target, Duration.ofSeconds(5))) {
            from = a.identity(topic, KafkaAccess.deadline(Duration.ofSeconds(5)), true);
            to = b.identity(topic, KafkaAccess.deadline(Duration.ofSeconds(5)), true);
        }
        NativeLog.write(source, EventCodec.raw(topic, event, attempt));
        try (var evidence = new JsonlEvidence(Path.of("build/reports/faultTest", "replication-"+seed+"-"+stage+".jsonl"))) {
            evidence.record("scenario", Map.of("seed", seed, "fault_point", stage, "source_side", seed % 2,
                    "attempt", attempt, "source", from, "target", to, "partition", event.partition()));
            CrashProcess.run(ReplicationCrashChild.class.getName(), "replication-"+seed+"-"+stage,
                    topic, PAIR.a.getBootstrapServers(), PAIR.b.getBootstrapServers(), stage, Integer.toString(seed));
            boolean committed = stage.equals("AFTER_SOURCE_COMMIT");
            long before = stage.equals("BEFORE_TARGET_APPEND") ? 0 : 1;
            assertEquals(before, NativeLog.read(target, topic).size());
            assertEquals(committed ? 1 : 0, NativeLog.read(source, config.confirmationTopic()).size());
            assertEquals(committed ? 1L : 0L, progress(source, config.groupId(from, to), topic, event.partition()));
            evidence.record("halted", Map.of("target_records", before, "visible_receipts", committed?1:0,
                    "source_next_offset", committed?1:0));
            try (var service = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
                service.start();
                await(service, seed % 2, event.partition(), 1);
                var receipts = NativeLog.read(source, config.confirmationTopic());
                assertEquals(1, receipts.size(), "orphan transaction must not expose its receipt");
                var ack = ReplicationAck.decode(receipts.getFirst(), 1_024);
                assertEquals(attempt, ack.attempt()); assertEquals(event.eventId(), ack.eventId());
                assertArrayEquals(EventCodec.hash(event.key(), event.payload()), ack.hash());
                assertEquals(from, ack.source()); assertEquals(to, ack.target()); assertEquals(0, ack.sourceOffset());
                var copies = NativeLog.read(target, topic);
                assertEquals(committed || before==0 ? 1 : 2, copies.size(), "only the uncommitted target append may replay");
                assertTrue(copies.stream().allMatch(r -> EventCodec.kind(r)==EventCodec.Kind.REPLICA
                        && EventCodec.event(r,1_024).equals(event)));
                assertTrue(copies.stream().anyMatch(r -> r.partition()==ack.partition() && r.offset()==ack.targetOffset()));
                assertEquals(1, progress(source, config.groupId(from, to), topic, event.partition()));
                evidence.record("ack", Map.of("evidence", "REPLICATION_ACK", "source_offset", ack.sourceOffset(),
                        "target_offset", ack.targetOffset(), "target_records", copies.size()));
            }
            long stable = NativeLog.read(target, topic).size();
            try (var restarted = new ReplicationService(config, PAIR.aProperties(), PAIR.bProperties())) {
                restarted.start(); await(restarted, seed % 2, event.partition(), 1);
                assertEquals(stable, NativeLog.read(target, topic).size());
            }
            EventAudit.read(source, List.of(topic), List.of(event)).requireComplete();
            EventAudit.read(target, List.of(topic), List.of(event)).requireComplete();
            evidence.record("verified", Map.of("seed", seed, "fault_point", stage, "source_next_offset",1,
                    "visible_receipts",1,"target_records",stable,"missing",0,"repeated_replication_growth",0));
        }
    }
    private static long progress(Map<String,Object> source, String group, String topic, int partition) throws Exception {
        try (var admin = Admin.create(source)) {
            var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS)
                    .get(new TopicPartition(topic,partition));
            return offset==null ? 0 : offset.offset();
        }
    }
    private static void await(ReplicationService service, int side, int partition, long offset) {
        long end=System.nanoTime()+Duration.ofSeconds(30).toNanos();
        while(System.nanoTime()<end) {
            var state=side==0?service.status().aToB():service.status().bToA();
            if(state.state()==ReplicationService.State.FAILED) { fail(state.toString()); }
            if(state.state()==ReplicationService.State.RUNNING && state.committedOffsets().getOrDefault(partition,0L)>=offset) { return; }
            LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
        }
        fail("source cursor did not recover: "+service.status());
    }
}

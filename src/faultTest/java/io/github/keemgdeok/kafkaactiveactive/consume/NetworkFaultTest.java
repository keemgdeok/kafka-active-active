package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.IntStream;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishResult;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairService;
import io.github.keemgdeok.kafkaactiveactive.support.FaultSeeds;
import io.github.keemgdeok.kafkaactiveactive.support.ProxyKafkaPair;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.EventDataset;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("try")
class NetworkFaultTest {
    private static final ProxyKafkaPair KAFKA = new ProxyKafkaPair();
    @BeforeAll static void start() throws Exception { KAFKA.start(); }
    @AfterAll static void stop() throws Exception { KAFKA.close(); }
    static IntStream seeds() { return FaultSeeds.values(); }
    @ParameterizedTest(name = "seed={0} lost ACK, asymmetric cut, hot key and live repair") @MethodSource("seeds")
    void responseLossAndAsymmetricIsolationDoNotInventSuccessOrLoseAcknowledgedEvents(int seed) throws Exception {
        String topic = "network-" + UUID.randomUUID();
        KAFKA.createTopic(topic);
        var events = EventDataset.generate(32, seed, true);
        var config = new PublishConfig(topic, Duration.ofSeconds(3), 64, 1_024, Duration.ofSeconds(3));
        var outcomes = new ArrayList<PublishResult>();
        try (var evidence = new JsonlEvidence(Path.of("build/reports/faultTest", "network-" + seed + ".jsonl"))) {
            evidence.record("scenario", Map.of("seed", seed, "topic", topic, "events", events.size(), "hot_key", true,
                    "response_timeout_ms", config.timeout().toMillis(), "max_pending", config.maxPending(),
                    "kafka_version", "4.3.2", "toxiproxy_version", "2.12.0"));
            try (var replication = ReplicationFixture.start(topic, KAFKA.aProxied(), KAFKA.bProxied());
                 var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(config, KAFKA.aProxied(), KAFKA.bProxied()))) {
                assertEquals(PublishResult.Status.CONFIRMED, send(publisher, events.get(0), outcomes, evidence).status());
                assertEquals(PublishResult.Status.CONFIRMED, send(publisher, events.get(1), outcomes, evidence).status());
                KAFKA.proxyB.toxics().timeout("lose-b-responses", ToxicDirection.DOWNSTREAM, 0);
                var lost = send(publisher, events.get(2), outcomes, evidence);
                assertEquals(PublishResult.Status.ACCEPTED_DEGRADED, lost.status());
                // The hot key routes to B.P1. B stored the RAW although its ACK was lost; A ACKed fallback.
                EventAudit.read(KAFKA.bDirect(), List.of(topic), events.subList(0, 3)).requireComplete();
                KAFKA.proxyA.toxics().timeout("lose-a-responses", ToxicDirection.DOWNSTREAM, 0);
                var unknown = send(publisher, events.get(3), outcomes, evidence);
                assertEquals(PublishResult.Status.UNKNOWN, unknown.status());
                assertFalse(EventAudit.read(KAFKA.aDirect(), List.of(topic), events.subList(0, 4)).missingIds().contains(unknown.eventId()),
                        "UNKNOWN is not rejection: fallback A stored this input without a visible ACK");
                KAFKA.heal();
                awaitDrained(publisher);
                // Healing the proxy does not instantly refresh an existing producer's metadata.
                // A healthy B ACK is valid success; the final real-log audit proves restoration.
                assertTrue(send(publisher, events.get(3), outcomes, evidence).successful());
                assertEquals(PublishResult.Status.UNKNOWN, unknown.status(), "late ACK must not rewrite returned evidence");
                assertEquals(PublishResult.Status.ACCEPTED_DEGRADED, lost.status());

                KAFKA.proxyA.disable(); // A is alive and direct observers remain connected.
                for (int i = 4; i < 6; i++) {
                    assertEquals(PublishResult.Status.ACCEPTED_DEGRADED, send(publisher, events.get(i), outcomes, evidence).status());
                }
                var beforeRepair = EventAudit.read(KAFKA.aDirect(), List.of(topic), events.subList(0, 6));
                assertTrue(beforeRepair.missingIds().containsAll(List.of(events.get(4).eventId(), events.get(5).eventId())));
                EventAudit.read(KAFKA.bDirect(), List.of(topic), events.subList(0, 6)).requireComplete();
                var settings = settings(topic);
                assertThrows(IllegalStateException.class,
                        () -> new GroupTransition(settings, KAFKA.aProxied(), KAFKA.bDirect(), () -> false).prepare());
                evidence.record("fault", Map.of("seed", seed, "point", "CLIENT_A_ISOLATED_BOTH_BROKERS_ALIVE", "missing_a", beforeRepair.missingIds().size()));
                KAFKA.heal(); awaitDrained(publisher);
                try (var repair = new RepairService(RepairConfig.defaults(topic), KAFKA.aDirect(), KAFKA.bDirect());
                     var executor = Executors.newSingleThreadExecutor()) {
                    var running = executor.submit(repair::runOnce);
                    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    while (!repair.status().running() && !running.isDone() && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(Duration.ofMillis(5).toNanos());
                    }
                    assertTrue(repair.status().running(), "input must overlap an active repair");
                    for (int i = 6; i < events.size(); i++) { assertTrue(send(publisher, events.get(i), outcomes, evidence).successful()); }
                    running.get(30, TimeUnit.SECONDS);
                    repair.runOnce();
                    var repeated = repair.runOnce(); assertEquals(0, repeated.copiedToA() + repeated.copiedToB());
                }
            }
            var a = EventAudit.read(KAFKA.aDirect(), List.of(topic), events);
            var b = EventAudit.read(KAFKA.bDirect(), List.of(topic), events);
            a.requireComplete(); b.requireComplete();
            for (var result : outcomes) {
                if (result.successful()) { assertFalse(a.missingIds().contains(result.eventId()) || b.missingIds().contains(result.eventId())); }
                if (result.status() == PublishResult.Status.CONFIRMED) {
                    assertEquals(PublishResult.Delivery.ACKNOWLEDGED, result.clusterA().delivery());
                    assertEquals(PublishResult.Delivery.ACKNOWLEDGED, result.clusterB().delivery());
                }
            }
            // Delay completion replication separately from input durability. One active native group remains on B.
            var settings = settings(topic);
            KAFKA.proxyA.disable();
            int calls = 0;
            try (var consumer = new ActiveKafkaConsumer(settings, KAFKA.bDirect(), KAFKA.aProxied())) {
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (consumer.status().completedIds() < events.size() && System.nanoTime() < deadline) {
                    var batch = consumer.poll(Duration.ofMillis(100));
                    for (var delivery : batch.records()) {
                        assertEquals(events.stream().filter(e -> e.eventId().equals(delivery.event().eventId())).findFirst().orElseThrow(), delivery.event());
                        calls++;
                        evidence.record("delivery", Map.of("seed", seed, "event_id", delivery.event().eventId(),
                                "active", "B", "partition", delivery.partition(), "offset", delivery.offset()));
                    }
                    if (!batch.isEmpty()) {
                        var result = consumer.commit(batch.processed()).toCompletableFuture().join();
                        assertEquals(CommitResult.Status.LOCAL_ONLY, result.status());
                        evidence.record("completion", Map.of("seed", seed, "status", result.status(),
                                "locally_completed_ids", result.locallyCompletedIds(), "peer_confirmed_ids", result.peerConfirmedIds()));
                    }
                }
                assertEquals(events.size(), consumer.status().completedIds());
            }
            assertEquals(events.size(), calls, "physical retry duplicates must not reach the SDK handler twice");
            KAFKA.heal();
            var transition = new GroupTransition(settings, KAFKA.bDirect(), KAFKA.aDirect(), () -> true).prepare();
            assertEquals(events.size(), transition.completionReport().completedIds());
            assertEquals(events.size(), transition.completionReport().copiedToB()); // source B -> target A
            try (var target = transition.startTarget()) {
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (target.status().filteredRecords() < a.physicalRecords() && System.nanoTime() < deadline) {
                    assertTrue(target.poll(Duration.ofMillis(100)).isEmpty(), "completed ID reached target handler");
                }
                assertEquals(0, target.status().deliveredEvents());
                assertTrue(target.status().filteredRecords() >= a.physicalRecords());
            }
            try (var ledgerA = new CompletionLedger(settings, KAFKA.aDirect());
                 var ledgerB = new CompletionLedger(settings, KAFKA.bDirect())) {
                assertEquals(events.size(), ledgerA.replay(ledgerA.snapshot()).size());
                assertEquals(events.size(), ledgerB.replay(ledgerB.snapshot()).size());
            }
            evidence.record("verified", Map.of("seed", seed, "events", events.size(), "handler_calls", calls,
                    "physical_records_a", a.physicalRecords(), "physical_records_b", b.physicalRecords(), "missing", 0,
                    "payload_conflicts", 0, "completed_redeliveries", 0, "key_order_inversions_a", a.keyOrderInversions(),
                    "key_order_inversions_b", b.keyOrderInversions()));
        } finally { KAFKA.heal(); }
    }
    @ParameterizedTest(name="seed={0} publisher receipt path isolated") @MethodSource("seeds")
    void readerDisconnectionCannotInventConfirmationOrReusePriorAttempt(int seed) throws Exception {
        String topic="reader-cut-"+UUID.randomUUID(); KAFKA.createTopic(topic);
        var warm=new Event("reader-warm-"+seed,"hello",new byte[]{0});
        var event=new Event("reader-event-"+seed,"hello",new byte[]{(byte)seed,1});
        var config=new PublishConfig(topic,Duration.ofSeconds(3),8,1_024,Duration.ofSeconds(5));
        var replication=ReplicationFixture.start(topic,KAFKA.aDirect(),KAFKA.bDirect());
        try(var evidence=new JsonlEvidence(Path.of("build/reports/faultTest","reader-"+seed+".jsonl"));
            var publisher=ReplicationFixture.ready(RoutedKafkaPublisher.open(config,KAFKA.aProxied(),KAFKA.bDirect()))) {
            assertEquals(PublishResult.Status.CONFIRMED,publisher.send(warm).toCompletableFuture().get(8,TimeUnit.SECONDS).status());
            KAFKA.proxyA.disable(); // Only publisher paths. The explicit worker still uses direct addresses.
            var isolated=publisher.send(event).toCompletableFuture().get(8,TimeUnit.SECONDS);
            assertEquals(PublishResult.Status.ACCEPTED_DEGRADED,isolated.status());
            long end=System.nanoTime()+Duration.ofSeconds(15).toNanos();
            while(replication.status().bToA().replicatedRecords()<2 && System.nanoTime()<end) {
                LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
            }
            assertEquals(2,replication.status().bToA().replicatedRecords());
            var before=io.github.keemgdeok.kafkaactiveactive.support.NativeLog.read(KAFKA.aDirect(),topic);
            assertEquals(2,before.size());
            assertTrue(before.stream().allMatch(r -> io.github.keemgdeok.kafkaactiveactive.internal.EventCodec.kind(r)
                    ==io.github.keemgdeok.kafkaactiveactive.internal.EventCodec.Kind.REPLICA));
            evidence.record("reader_cut",Map.of("seed",seed,"status",isolated.status(),"native_peer_records",before.size(),
                    "a_evidence",isolated.clusterA().evidence(),"b_evidence",isolated.clusterB().evidence()));
            replication.close(); KAFKA.heal(); ReplicationFixture.ready(publisher);
            var retry=publisher.send(event).toCompletableFuture().get(8,TimeUnit.SECONDS);
            assertEquals(PublishResult.Status.ACCEPTED_DEGRADED,retry.status(),"old receipt confirmed a new RAW attempt");
            assertEquals(2,io.github.keemgdeok.kafkaactiveactive.support.NativeLog.read(KAFKA.aDirect(),topic).size());
            assertEquals(3,io.github.keemgdeok.kafkaactiveactive.support.NativeLog.read(KAFKA.bDirect(),topic).size());
            try(var resumed=ReplicationFixture.start(topic,KAFKA.aDirect(),KAFKA.bDirect())) {
                end=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                while(resumed.status().bToA().committedOffsets().getOrDefault(1,0L)<3 && System.nanoTime()<end) {
                    LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
                }
                assertEquals(3,resumed.status().bToA().committedOffsets().getOrDefault(1,0L));
            }
            EventAudit.read(KAFKA.aDirect(),List.of(topic),List.of(warm,event)).requireComplete();
            EventAudit.read(KAFKA.bDirect(),List.of(topic),List.of(warm,event)).requireComplete();
            assertEquals(PublishResult.Status.ACCEPTED_DEGRADED,isolated.status());
            evidence.record("verified",Map.of("seed",seed,"missing",0,"old_receipt_reused",false,
                    "extra_raw_on_peer",0,"immutable_degraded",true));
        } finally { replication.close(); KAFKA.heal(); }
    }

    private static ConsumerSettings settings(String topic) {
        return new ConsumerSettings(topic, "group-" + UUID.randomUUID(), Duration.ofSeconds(3), 100, 1_000, 1_000_000, 1_024);
    }
    private static PublishResult send(RoutedKafkaPublisher publisher, Event event, List<PublishResult> outcomes,
                                       JsonlEvidence evidence) throws Exception {
        long start = System.nanoTime();
        var result = publisher.send(event).toCompletableFuture().get(8, TimeUnit.SECONDS);
        outcomes.add(result);
        evidence.record("attempt", Map.of("event_id", event.eventId(), "partition", event.partition(), "status", result.status(),
                "a_delivery", result.clusterA().delivery(), "b_delivery", result.clusterB().delivery(),
                "a_error", String.valueOf(result.clusterA().errorType()), "b_error", String.valueOf(result.clusterB().errorType()),
                "elapsed_ms", (System.nanoTime() - start) / 1_000_000,
                "a_evidence", result.clusterA().evidence(), "b_evidence", result.clusterB().evidence()));
        return result;
    }
    private static void awaitDrained(RoutedKafkaPublisher publisher) {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (publisher.pendingRequests() > 0 && System.nanoTime() < deadline) { LockSupport.parkNanos(Duration.ofMillis(20).toNanos()); }
        assertEquals(0, publisher.pendingRequests());
    }
}

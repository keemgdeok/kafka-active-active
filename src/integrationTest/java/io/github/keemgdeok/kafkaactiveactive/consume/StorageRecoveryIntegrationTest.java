package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishResult;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.EventDataset;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;

@SuppressWarnings("try")
class StorageRecoveryIntegrationTest {
    @Test void replacingStorageRestoresBothDataAndCompletionsBeforeNewInputIsDelivered() throws Exception {
        int port;
        try (var reservation = new ServerSocket(0)) { port = reservation.getLocalPort(); }
        var replacement = KafkaPair.broker("RcK8mGojT1n2M9p8Zq4Xch");
        pin(replacement, port);
        var events = EventDataset.generate(8, 42, false);
        String topic = "storage-" + UUID.randomUUID();
        var settings = new ConsumerSettings(topic, "storage-group-" + UUID.randomUUID(), Duration.ofSeconds(5),
                100, 1_000, 1_000_000, 1_024);
        Path diagnostics = Path.of("build/reports/integrationTest/storage-recovery");
        Files.createDirectories(diagnostics);
        try (var pair = new KafkaPair("storage-survivor");
             var evidence = new JsonlEvidence(diagnostics.resolve("events.jsonl"))) {
            pin(pair.a, port); pair.start(); pair.createTopic(topic);
            Map<String, Object> originalAddress = pair.aProperties();
            try (var replication = ReplicationFixture.start(topic, originalAddress, pair.bProperties());
                 var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(PublishConfig.defaults(topic), originalAddress, pair.bProperties()))) {
                for (var event : events) { assertEquals(PublishResult.Status.CONFIRMED,
                        publisher.send(event).toCompletableFuture().get(10, TimeUnit.SECONDS).status()); }
            }
            var old = new ActiveKafkaConsumer(settings, originalAddress, pair.bProperties());
            try (old) {
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (old.status().completedIds() < events.size() && System.nanoTime() < deadline) {
                    var batch = old.poll(Duration.ofMillis(100));
                    for (var record : batch.records()) { assertTrue(events.contains(record.event())); }
                    if (!batch.isEmpty()) { assertEquals(CommitResult.Status.CONFIRMED, old.commit(batch.processed()).toCompletableFuture().join().status()); }
                }
                assertEquals(events.size(), old.status().completedIds());
            }
            String oldCluster;
            String oldInput;
            String oldLedger;
            try (var ledger = new CompletionLedger(settings, originalAddress)) {
                var boundary = ledger.snapshot();
                oldCluster = boundary.clusterId(); oldLedger = boundary.topicId(); oldInput = ledger.businessTopicId();
            }
            Files.writeString(diagnostics.resolve("kafka-a-before.log"), pair.a.getLogs());
            pair.a.stop(); // Remove the container and all of its ephemeral Kafka storage, including group offsets.
            replacement.start();
            var restoredAddress = Map.<String, Object>of("bootstrap.servers", replacement.getBootstrapServers());
            assertEquals(originalAddress, restoredAddress);
            try (var admin = Admin.create(restoredAddress)) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short) 1).configs(Map.of(
                        "cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1"))))
                        .all().get(10, TimeUnit.SECONDS);
            }
            var prepared = new GroupTransition(settings, pair.bProperties(), restoredAddress, () -> old.status().closed()).prepare();
            assertEquals(events.size(), prepared.dataReport().copiedToB());
            assertEquals(events.size(), prepared.completionReport().copiedToB());
            try (var ledger = new CompletionLedger(settings, restoredAddress)) {
                var boundary = ledger.snapshot();
                assertNotEquals(oldCluster, boundary.clusterId()); assertNotEquals(oldInput, ledger.businessTopicId());
                assertNotEquals(oldLedger, boundary.topicId()); assertEquals(events.size(), ledger.replay(boundary).size());
                evidence.record("restored", Map.of("same_bootstrap", true, "old_cluster", oldCluster, "new_cluster", boundary.clusterId(),
                        "old_input", oldInput, "new_input", ledger.businessTopicId(), "old_ledger", oldLedger, "new_ledger", boundary.topicId(),
                        "data_copied", prepared.dataReport().copiedToB(), "completions_copied", prepared.completionReport().copiedToB()));
            }
            var fresh = new Event("after-storage-restore", "fresh-key", new byte[] {0, -1, 42});
            try (var target = prepared.startTarget()) {
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (target.status().filteredRecords() < events.size() && System.nanoTime() < deadline) {
                    assertTrue(target.poll(Duration.ofMillis(100)).isEmpty(), "completed ID reached handler after storage replacement");
                }
                assertTrue(target.status().filteredRecords() >= events.size()); assertEquals(0, target.status().deliveredEvents());
                try (var replication = ReplicationFixture.start(topic, restoredAddress, pair.bProperties());
                 var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(PublishConfig.defaults(topic), restoredAddress, pair.bProperties()))) {
                    assertEquals(PublishResult.Status.CONFIRMED, publisher.send(fresh).toCompletableFuture().get(10, TimeUnit.SECONDS).status());
                }
                var batch = awaitBatch(target);
                assertEquals(List.of(fresh), batch.records().stream().map(Batch.Delivery::event).toList());
                assertEquals(CommitResult.Status.CONFIRMED, target.commit(batch.processed()).toCompletableFuture().join().status());
            }
            var expected = new ArrayList<>(events); expected.add(fresh);
            for (var properties : List.of(restoredAddress, pair.bProperties())) {
                EventAudit.read(properties, List.of(topic), expected).requireComplete();
                try (var ledger = new CompletionLedger(settings, properties)) { assertEquals(expected.size(), ledger.replay(ledger.snapshot()).size()); }
            }
            evidence.record("verified", Map.of("restored_completed_ids", events.size(), "completed_redeliveries", 0,
                    "new_handler_calls", 1, "data_and_completion_ids_both", expected.size(), "missing", 0, "payload_conflicts", 0));
        } finally {
            try { if (replacement.isRunning()) { Files.writeString(diagnostics.resolve("kafka-a-after.log"), replacement.getLogs()); } }
            finally { replacement.stop(); }
        }
    }
    private static void pin(KafkaContainer kafka, int port) {
        kafka.withCreateContainerCmdModifier(command -> command.getHostConfig().withPortBindings(
                new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(9092))));
    }
    private static Batch awaitBatch(ActiveKafkaConsumer consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (System.nanoTime() < deadline) {
            var batch = consumer.poll(Duration.ofMillis(100)); if (!batch.isEmpty()) { return batch; }
        }
        throw new AssertionError("new input was not delivered after storage recovery: " + consumer.status());
    }
}

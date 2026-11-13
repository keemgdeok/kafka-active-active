package io.github.keemgdeok.kafkaactiveactive.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.NativeLog;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;

import org.junit.jupiter.api.Test;

@SuppressWarnings("try")
class ProducerExampleIntegrationTest {
    @Test
    void designatedProducerReplicatesBothWaysAndRestartsWhileTheOtherProducerKeepsPublishing() throws Exception {
        try (var kafka = new KafkaPair("producer-example")) {
            kafka.start();
            String topic = "producer-example-" + UUID.randomUUID();
            kafka.createTopic(topic);
            var env = Map.of("KAA_TOPIC", topic, "KAA_BOOTSTRAP_A", kafka.a.getBootstrapServers(),
                    "KAA_BOOTSTRAP_B", kafka.b.getBootstrapServers());
            var ownerEnv = new HashMap<>(env);
            ownerEnv.put("KAA_REPLICATION_ENABLED", "true");
            String confirmationTopic = ReplicationConfig.defaults(topic).confirmationTopic();
            var first = event("before-owner", "kafka");
            var fromOwner = event("from-owner", "hello");
            var fromPublisher = event("from-publisher", "kafka");
            var pending = event("between-owners", "kafka");

            try (var publisher = new RunningExample(env)) {
                assertEquals("replication=false", publisher.nextLine());
                assertEquals(first.eventId() + "\tACCEPTED_DEGRADED", publisher.send(first));
                assertTrue(NativeLog.read(kafka.bProperties(), topic).isEmpty());

                try (var owner = new RunningExample(ownerEnv)) {
                    assertEquals("replication=true", owner.nextLine());
                    assertAccepted(fromOwner, owner.send(fromOwner));
                    assertAccepted(fromPublisher, publisher.send(fromPublisher));
                    NativeLog.awaitVisibleCount(kafka.aProperties(), confirmationTopic, 2);
                    NativeLog.awaitVisibleCount(kafka.bProperties(), confirmationTopic, 1);
                    auditBoth(kafka, topic, List.of(first, fromOwner, fromPublisher));
                }

                var events = List.of(first, fromOwner, fromPublisher, pending);
                assertEquals(pending.eventId() + "\tACCEPTED_DEGRADED", publisher.send(pending));
                assertEquals(Set.of(pending.eventId()), EventAudit.read(kafka.bProperties(), List.of(topic), events).missingIds());

                try (var replacement = new RunningExample(ownerEnv)) {
                    assertEquals("replication=true", replacement.nextLine());
                    NativeLog.awaitVisibleCount(kafka.aProperties(), confirmationTopic, 3);
                    auditBoth(kafka, topic, events);
                }
            }
        }
    }

    private static Event event(String id, String key) {
        return new Event(id, key, id.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertAccepted(Event event, String line) {
        assertTrue(Set.of(event.eventId() + "\tCONFIRMED", event.eventId() + "\tACCEPTED_DEGRADED").contains(line), line);
    }

    private static void auditBoth(KafkaPair kafka, String topic, List<Event> events) {
        for (var properties : List.of(kafka.aProperties(), kafka.bProperties())) {
            var report = EventAudit.read(properties, List.of(topic), events);
            report.requireComplete();
            assertEquals(events.size(), report.physicalRecords());
        }
    }

    private static final class RunningExample implements AutoCloseable {
        private final PipedWriter input = new PipedWriter();
        private final LinkedBlockingQueue<String> output = new LinkedBlockingQueue<>();
        private final FutureTask<Void> running;

        RunningExample(Map<String, String> env) throws IOException {
            var reader = new BufferedReader(new PipedReader(input));
            running = new FutureTask<>(() -> {
                try (reader) { ProducerExample.run(env, reader, output::add); }
                return null;
            });
            Thread.ofVirtual().start(running);
        }

        String send(Event event) throws Exception {
            input.write(event.eventId() + "\t" + event.key() + "\t"
                    + new String(event.payload(), StandardCharsets.UTF_8) + "\n");
            input.flush();
            return nextLine();
        }

        String nextLine() throws Exception {
            String line = output.poll(15, TimeUnit.SECONDS);
            if (line == null && running.isDone()) {
                running.get();
            }
            assertNotNull(line, "producer example did not respond");
            return line;
        }

        @Override public void close() throws Exception {
            input.close();
            running.get(20, TimeUnit.SECONDS);
        }
    }
}

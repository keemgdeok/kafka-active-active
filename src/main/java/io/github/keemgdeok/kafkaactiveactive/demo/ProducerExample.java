package io.github.keemgdeok.kafkaactiveactive.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationService;

/**
 * Producer application example. Set KAA_REPLICATION_ENABLED=true on one process per topic and
 * Kafka pair. Each input line contains event ID, key and UTF-8 payload separated by tabs.
 * End input to close the publisher and the replication service it owns.
 */
public final class ProducerExample {
    private ProducerExample() {}

    public static void main(String[] args) throws IOException {
        try (var input = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            run(System.getenv(), input, System.out::println);
        }
    }

    static void run(Map<String, String> environment, BufferedReader input, Consumer<String> output) throws IOException {
        boolean ownsReplication = switch (environment.getOrDefault("KAA_REPLICATION_ENABLED", "false")) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("KAA_REPLICATION_ENABLED must be true or false");
        };
        String topic = environment.getOrDefault("KAA_TOPIC", "events");
        var a = Map.<String, Object>of("bootstrap.servers",
                environment.getOrDefault("KAA_BOOTSTRAP_A", "localhost:19092"));
        var b = Map.<String, Object>of("bootstrap.servers",
                environment.getOrDefault("KAA_BOOTSTRAP_B", "localhost:29092"));

        try (var replication = ownsReplication ? new ReplicationService(ReplicationConfig.defaults(topic), a, b) : null;
             var publisher = RoutedKafkaPublisher.open(PublishConfig.defaults(topic), a, b)) {
            if (replication != null) {
                replication.start();
            }
            output.accept("replication=" + ownsReplication);

            String line;
            while ((line = input.readLine()) != null) {
                String[] fields = line.split("\t", 3);
                if (fields.length != 3) {
                    throw new IllegalArgumentException("expected event ID, key and payload separated by tabs");
                }
                var event = new Event(fields[0], fields[1], fields[2].getBytes(StandardCharsets.UTF_8));
                var result = publisher.send(event).toCompletableFuture().join();
                output.accept(result.eventId() + "\t" + result.status());
            }
        }
    }
}

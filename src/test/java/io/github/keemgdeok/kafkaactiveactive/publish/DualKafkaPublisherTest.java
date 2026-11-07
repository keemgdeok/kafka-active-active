package io.github.keemgdeok.kafkaactiveactive.publish;

import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Delivery.ACKNOWLEDGED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Delivery.FAILED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Delivery.UNCONFIRMED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.ACCEPTED_DEGRADED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.CONFIRMED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.REJECTED;
import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DualKafkaPublisherTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Event EVENT = new Event("event-1", "customer-1", new byte[] {1, 2, 3});
    private final AtomicLong clock = new AtomicLong();
    private final ControlledProducer a = new ControlledProducer();
    private final ControlledProducer b = new ControlledProducer();
    private final DualKafkaPublisher publisher = new DualKafkaPublisher(
            new PublishConfig("events", TIMEOUT, 1, 128, Duration.ofMillis(200)), a, b, clock::get);

    @AfterEach
    void close() {
        a.unblock.countDown();
        b.unblock.countDown();
        publisher.close();
    }

    @Test
    void bothAcksAreRequiredForConfirmedAndRecordBytesAreIdentical() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture();
        var recordA = a.next();
        var recordB = b.next();
        assertEquals(EVENT.partition(), recordA.partition());
        assertEquals(recordA.partition(), recordB.partition());
        assertArrayEquals(recordA.key(), recordB.key());
        assertArrayEquals(EVENT.payload(), recordA.value());
        assertArrayEquals(recordA.value(), recordB.value());
        assertEquals(EVENT.eventId(), new String(recordA.headers().lastHeader(
                DualKafkaPublisher.EVENT_ID_HEADER).value(), StandardCharsets.UTF_8));
        assertArrayEquals(recordA.headers().lastHeader(DualKafkaPublisher.EVENT_ID_HEADER).value(),
                recordB.headers().lastHeader(DualKafkaPublisher.EVENT_ID_HEADER).value());
        b.completeNext();
        assertFalse(response.isDone());
        a.completeNext();
        var result = response.get(2, TimeUnit.SECONDS);
        assertEquals(CONFIRMED, result.status());
        assertTrue(result.successful());
        assertEquals(ACKNOWLEDGED, result.clusterA().delivery());
        assertEquals(ACKNOWLEDGED, result.clusterB().delivery());
        assertNotNull(result.clusterA().offset());
        assertEquals(0, publisher.pendingRequests());
    }

    @Test
    void oneAckAndOneFailureAreFinalDegradedSuccess() throws Exception {
        var response = submitted();
        a.errorNext(new TimeoutException());
        b.completeNext();
        var result = response.get(2, TimeUnit.SECONDS);
        assertEquals(ACCEPTED_DEGRADED, result.status());
        assertTrue(result.successful());
        assertEquals(FAILED, result.clusterA().delivery());
        assertEquals("TimeoutException", result.clusterA().errorType());
        assertEquals(ACKNOWLEDGED, result.clusterB().delivery());
    }

    @Test
    void bothFailuresAreUnknownRatherThanRejected() throws Exception {
        var response = submitted();
        a.errorNext(new TimeoutException());
        b.errorNext(new TimeoutException());
        assertEquals(UNKNOWN, response.get(2, TimeUnit.SECONDS).status());
        assertFalse(response.join().successful());
    }

    @Test
    void deadlineWithOneAckReturnsDegradedAndLateAckCannotMutateHistory() throws Exception {
        var response = submitted();
        a.completeNext();
        clock.set(TIMEOUT.toNanos());
        publisher.expirePending();
        var snapshot = response.get(2, TimeUnit.SECONDS);
        assertEquals(ACCEPTED_DEGRADED, snapshot.status());
        assertEquals(UNCONFIRMED, snapshot.clusterB().delivery());
        assertEquals(1, publisher.pendingRequests());
        assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status());
        b.completeNext();
        assertEquals(snapshot, response.join());
        assertEquals(0, publisher.pendingRequests());
    }

    @Test
    void lateAckCannotBecomeEvidenceEvenWhenTimerHasNotRun() throws Exception {
        var response = submitted();
        clock.set(TIMEOUT.toNanos());
        a.completeNext();
        b.completeNext();
        var result = response.get(2, TimeUnit.SECONDS);
        assertEquals(UNKNOWN, result.status());
        assertEquals(UNCONFIRMED, result.clusterA().delivery());
        assertEquals(UNCONFIRMED, result.clusterB().delivery());
    }

    @Test
    void ackBeforeDeadlineCountsAndAckAtDeadlineDoesNot() throws Exception {
        var response = submitted();
        clock.set(TIMEOUT.toNanos() - 1);
        a.completeNext();
        publisher.expirePending();
        assertFalse(response.isDone());
        clock.incrementAndGet();
        b.completeNext();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
    }

    @Test
    void deadlineWithoutAnyAckReturnsUnknownAndKeepsAdmissionBounded() throws Exception {
        var response = submitted();
        clock.set(TIMEOUT.toNanos());
        publisher.expirePending();
        assertEquals(UNKNOWN, response.get(2, TimeUnit.SECONDS).status());
        for (int i = 0; i < 100; i++) {
            assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status());
        }
        assertEquals(1, a.history().size());
        assertEquals(1, b.history().size());
        a.completeNext();
        b.completeNext();
        var retry = submitted();
        a.completeNext();
        b.completeNext();
        assertEquals(CONFIRMED, retry.get(2, TimeUnit.SECONDS).status());
        assertArrayEquals(a.history().getFirst().value(), a.history().getLast().value());
        assertArrayEquals(a.history().getFirst().headers().lastHeader(DualKafkaPublisher.EVENT_ID_HEADER).value(),
                a.history().getLast().headers().lastHeader(DualKafkaPublisher.EVENT_ID_HEADER).value());
    }

    @Test
    void rejectedOversizedEventNeverReachesEitherProducer() {
        var result = publisher.send(new Event("large", "key", new byte[128])).toCompletableFuture().join();
        assertEquals(REJECTED, result.status());
        assertEquals(0, publisher.pendingRequests());
        assertTrue(a.history().isEmpty());
        assertTrue(b.history().isEmpty());
    }

    @Test
    void metadataBlockOnOneSideDoesNotBlockOtherSideOrStartExpiredWrite() throws Exception {
        a.blockMetadata = true;
        var response = publisher.send(EVENT).toCompletableFuture();
        assertTrue(a.metadataEntered.await(2, TimeUnit.SECONDS));
        b.next();
        b.completeNext();
        clock.set(TIMEOUT.toNanos());
        publisher.expirePending();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        a.unblock.countDown();
        assertTrue(a.metadataReturned.await(2, TimeUnit.SECONDS));
        publisher.close();
        assertTrue(a.history().isEmpty());
    }

    @Test
    void rejectsWrongPartitionCountBeforeNativeSend() throws Exception {
        b.wrongTopology = true;
        var response = publisher.send(EVENT).toCompletableFuture();
        a.next();
        a.completeNext();
        var result = response.get(2, TimeUnit.SECONDS);
        assertEquals(ACCEPTED_DEGRADED, result.status());
        assertEquals("IllegalStateException", result.clusterB().errorType());
        assertTrue(b.history().isEmpty());
    }

    @Test
    void synchronousSendFailureDoesNotPreventOtherSideFromAcknowledging() throws Exception {
        a.sendException = new IllegalStateException("failed client");
        var response = publisher.send(EVENT).toCompletableFuture();
        b.next();
        b.completeNext();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
    }

    @Test
    void closeResolvesPendingRequestsConservativelyAndRejectsNewWrites() throws Exception {
        var response = submitted();
        a.completeNext();
        publisher.close();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        assertEquals(0, publisher.pendingRequests());
        assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status());
        assertTrue(a.closed());
        assertTrue(b.closed());
    }

    @Test
    void callerCannotCompleteSdkOwnedFutureToReleaseCapacity() throws Exception {
        var exposed = submitted();
        exposed.cancel(false);
        assertEquals(1, publisher.pendingRequests());
        assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status());
        a.completeNext();
        b.completeNext();
        assertEquals(0, publisher.pendingRequests());
    }

    @Test
    void nativeConfigCannotWeakenAcknowledgementsOrModifyRecords() {
        Map<String, Object> supplied = Map.of("bootstrap.servers", "a:9092", "acks", "0",
                "enable.idempotence", false, "sasl.mechanism", "PLAIN");
        var effective = DualKafkaPublisher.producerProperties(PublishConfig.defaults("events"), supplied);
        assertEquals("all", effective.get("acks"));
        assertEquals(true, effective.get("enable.idempotence"));
        assertEquals("PLAIN", effective.get("sasl.mechanism"));
        assertEquals("0", supplied.get("acks"));
        assertThrows(IllegalArgumentException.class, () -> DualKafkaPublisher.producerProperties(
                PublishConfig.defaults("events"), Map.of("bootstrap.servers", "a:9092", "transactional.id", "tx")));
    }

    @Test
    void concurrentCallersCannotExceedAdmissionLimitOrPartiallyPublishRejectedEvents() throws Exception {
        var left = new ControlledProducer();
        var right = new ControlledProducer();
        try (var bounded = new DualKafkaPublisher(new PublishConfig("events", TIMEOUT, 4, 128,
                Duration.ofMillis(200)), left, right, clock::get);
             var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new CountDownLatch(1);
            var tasks = new java.util.ArrayList<Future<CompletableFuture<PublishResult>>>();
            for (int i = 0; i < 40; i++) {
                int id = i;
                tasks.add(callers.submit(() -> {
                    start.await();
                    return bounded.send(new Event("id-" + id, "key", new byte[] {1})).toCompletableFuture();
                }));
            }
            start.countDown();
            var responses = new java.util.ArrayList<CompletableFuture<PublishResult>>();
            for (var task : tasks) {
                responses.add(task.get(2, TimeUnit.SECONDS));
            }
            assertEquals(4, bounded.pendingRequests());
            for (int i = 0; i < 4; i++) {
                left.next();
                right.next();
            }
            for (int i = 0; i < 4; i++) {
                left.completeNext();
                right.completeNext();
            }
            assertEquals(4, responses.stream().map(CompletableFuture::join)
                    .filter(r -> r.status() == CONFIRMED).count());
            assertEquals(36, responses.stream().map(CompletableFuture::join)
                    .filter(r -> r.status() == REJECTED).count());
            for (int i = 0; i < 4; i++) {
                assertArrayEquals(left.history().get(i).headers().lastHeader(DualKafkaPublisher.EVENT_ID_HEADER).value(),
                        right.history().get(i).headers().lastHeader(DualKafkaPublisher.EVENT_ID_HEADER).value());
            }
        }
    }

    private CompletableFuture<PublishResult> submitted() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture();
        a.next();
        b.next();
        return response;
    }

    private static final class ControlledProducer extends MockProducer<byte[], byte[]> {
        private final LinkedBlockingQueue<ProducerRecord<byte[], byte[]>> sends = new LinkedBlockingQueue<>();
        private final CountDownLatch metadataEntered = new CountDownLatch(1);
        private final CountDownLatch metadataReturned = new CountDownLatch(1);
        private final CountDownLatch unblock = new CountDownLatch(1);
        private volatile boolean blockMetadata;
        private volatile boolean wrongTopology;

        private ControlledProducer() {
            super(cluster(), false, null, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            metadataEntered.countDown();
            if (blockMetadata) {
                try {
                    if (!unblock.await(3, TimeUnit.SECONDS)) {
                        throw new AssertionError("metadata was not released");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(failure);
                }
            }
            metadataReturned.countDown();
            var result = super.partitionsFor(topic);
            return wrongTopology ? result.subList(0, 1) : result;
        }

        @Override
        public synchronized Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                ProducerRecord<byte[], byte[]> record, Callback callback) {
            var result = super.send(record, callback);
            sends.add(record);
            return result;
        }

        private ProducerRecord<byte[], byte[]> next() throws InterruptedException {
            var record = sends.poll(2, TimeUnit.SECONDS);
            assertNotNull(record, "native send was not called");
            return record;
        }

        private static Cluster cluster() {
            Node node = new Node(1, "localhost", 9092);
            Node[] replicas = {node};
            return new Cluster("test", List.of(node), List.of(
                    new PartitionInfo("events", 0, node, replicas, replicas),
                    new PartitionInfo("events", 1, node, replicas, replicas)), Set.of(), Set.of());
        }
    }
}

package io.github.keemgdeok.kafkaactiveactive.publish;

import static io.github.keemgdeok.kafkaactiveactive.publish.PublishResult.Status.*;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.replication.LogIdentity;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationAck;

class RoutedKafkaPublisherTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Event EVENT = new Event("event-1", "kafka", new byte[] {1, 2, 3});
    private static final LogIdentity A = new LogIdentity("a", "topic-a");
    private static final LogIdentity B = new LogIdentity("b", "topic-b");
    private static final AcknowledgementReader.Generation AB = new AcknowledgementReader.Generation(A, B);
    private static final AcknowledgementReader.Generation BA = new AcknowledgementReader.Generation(B, A);
    private final AtomicLong clock = new AtomicLong();
    private final ControlledProducer a = new ControlledProducer();
    private final ControlledProducer b = new ControlledProducer();
    private final RoutedKafkaPublisher publisher = new RoutedKafkaPublisher(
            new PublishConfig("events", TIMEOUT, 1, 128, Duration.ofMillis(200)), a, b, clock::get);

    RoutedKafkaPublisherTest() { publisher.observe(0, AB); publisher.observe(1, BA); }
    @AfterEach void close() { a.unblock.countDown(); b.unblock.countDown(); publisher.close(); }

    @Test void preferredRawAndReplicaAckConfirmTwoStoresWithoutSecondRaw() throws Exception {
        assertEquals(0, EVENT.partition());
        var response = publisher.send(EVENT).toCompletableFuture();
        var raw = a.next(); a.completeNext();
        clock.set(Duration.ofSeconds(2).toNanos()); publisher.expirePending();
        assertTrue(b.history().isEmpty(), "receipt lag must not trigger RAW fallback");
        assertFalse(response.isDone());
        receipt(0, ack(raw, EVENT, 0, 7));
        var result = response.get(2, TimeUnit.SECONDS);
        assertEquals(CONFIRMED, result.status());
        assertEquals(PublishResult.Evidence.DIRECT_RAW_ACK, result.clusterA().evidence());
        assertEquals(PublishResult.Evidence.REPLICATION_ACK, result.clusterB().evidence());
        assertEquals(7L, result.clusterB().offset()); assertEquals(0, publisher.pendingRequests());
        assertArrayEquals(new byte[]{2}, raw.headers().lastHeader(EventCodec.FORMAT_HEADER).value());
    }
    @Test void partitionOneStartsOnBAndConfirmationCountsAReplica() throws Exception {
        var event = new Event("b-raw", "hello", new byte[]{1}); assertEquals(1, event.partition());
        var response = publisher.send(event).toCompletableFuture(); var raw = b.next(); b.completeNext();
        receipt(1, ack(raw, event, 0, 9));
        assertEquals(CONFIRMED, response.get(2, TimeUnit.SECONDS).status()); assertTrue(a.history().isEmpty());
    }
    @Test void earlyReceiptNeverInventsSourceRawAck() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); var raw = a.next();
        receipt(0, ack(raw, EVENT, 0, 8)); assertFalse(response.isDone());
        a.completeNext(); assertEquals(CONFIRMED, response.get(2, TimeUnit.SECONDS).status());
    }
    @Test void replicaReceiptAndFallbackRawAckStillCannotInventMissingPreferredRawAck() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); var raw = a.next();
        receipt(0, ack(raw, EVENT, 0, 4)); fallback(); b.next(); b.completeNext(); expire();
        var result = response.get(2, TimeUnit.SECONDS);
        assertEquals(ACCEPTED_DEGRADED, result.status());
        assertEquals(PublishResult.Delivery.UNCONFIRMED, result.clusterA().delivery());
        assertEquals(PublishResult.Evidence.DIRECT_RAW_ACK, result.clusterB().evidence());
        a.completeNext(); assertEquals(result, response.join());
    }
    @Test void absentPreferredAckFallsBackOnceWithSameIdentityAndNewAttempt() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); var left = a.next();
        fallback(); var right = b.next(); publisher.expirePending();
        assertArrayEquals(left.key(), right.key()); assertArrayEquals(left.value(), right.value());
        assertArrayEquals(left.headers().lastHeader(EventCodec.EVENT_ID_HEADER).value(),
                right.headers().lastHeader(EventCodec.EVENT_ID_HEADER).value());
        assertFalse(java.util.Arrays.equals(left.headers().lastHeader(EventCodec.ATTEMPT_HEADER).value(),
                right.headers().lastHeader(EventCodec.ATTEMPT_HEADER).value()));
        assertEquals(0, right.partition()); assertEquals(1, b.history().size());
        b.completeNext(); a.completeNext(); assertEquals(CONFIRMED, response.get(2, TimeUnit.SECONDS).status());
    }
    @Test void preferredFailureFallsBackImmediatelyAndOneAckIsDegradedAtDeadline() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); a.errorNext(new TimeoutException());
        b.next(); b.completeNext(); expire();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        assertEquals("TimeoutException", response.join().clusterA().errorType());
    }
    @Test void bothFailuresAreUnknownAndNeverRejected() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); a.errorNext(new TimeoutException());
        b.next(); b.errorNext(new TimeoutException()); assertEquals(UNKNOWN, response.get(2, TimeUnit.SECONDS).status());
    }
    @Test void receiptLagEndsDegradedAndLateReceiptCannotChangeResult() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); var raw = a.next(); a.completeNext(); expire();
        var snapshot = response.get(2, TimeUnit.SECONDS); assertEquals(ACCEPTED_DEGRADED, snapshot.status());
        assertEquals(0, publisher.pendingRequests()); receipt(0, ack(raw, EVENT, 0, 3));
        assertEquals(snapshot, response.join()); assertTrue(b.history().isEmpty());
    }
    @Test void pendingNativeWritesRetainCapacityAfterResponseAndLateAcksCannotChangeIt() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); fallback(); b.next(); expire();
        var snapshot = response.get(2, TimeUnit.SECONDS); assertEquals(UNKNOWN, snapshot.status());
        for (int i=0;i<100;i++) { assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status()); }
        a.completeNext(); assertEquals(1, publisher.pendingRequests()); b.completeNext();
        assertEquals(0, publisher.pendingRequests()); assertEquals(snapshot, response.join());
    }
    @Test void callbackAtDeadlineIsNotEvidenceEvenBeforeTimerRuns() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); fallback(); b.next();
        clock.set(TIMEOUT.toNanos()); a.completeNext(); b.completeNext();
        assertEquals(UNKNOWN, response.get(2, TimeUnit.SECONDS).status());
    }
    @Test void ackBeforeDeadlineCountsButAckAtDeadlineDoesNot() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); fallback(); b.next();
        clock.set(TIMEOUT.toNanos()-1); a.completeNext(); clock.incrementAndGet(); b.completeNext();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
    }
    @Test void invalidReceiptsAndPreviousAttemptAreNeverReused() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); var raw = a.next(); a.completeNext();
        var valid = ack(raw, EVENT, 0, 4);
        for (var invalid : List.of(
                new ReplicationAck(valid.attempt(), "wrong", valid.hash(), 0, A, 0, B, 4),
                new ReplicationAck(valid.attempt(), EVENT.eventId(), new byte[32], 0, A, 0, B, 4),
                new ReplicationAck(valid.attempt(), EVENT.eventId(), valid.hash(), 1, A, 0, B, 4),
                new ReplicationAck(valid.attempt(), EVENT.eventId(), valid.hash(), 0, A, 2, B, 4),
                new ReplicationAck(valid.attempt(), EVENT.eventId(), valid.hash(), 0, new LogIdentity("a", "old"), 0, B, 4),
                new ReplicationAck(valid.attempt(), EVENT.eventId(), valid.hash(), 0, A, 0, new LogIdentity("b", "old"), 4))) {
            receipt(0, invalid); assertFalse(response.isDone());
        }
        assertEquals(6, publisher.status().invalidAcknowledgements()); expire(); response.get(2, TimeUnit.SECONDS);
        var retry = publisher.send(EVENT).toCompletableFuture(); a.next(); a.completeNext(); receipt(0, valid);
        assertFalse(retry.isDone()); publisher.close();
    }
    @Test void missingReaderGenerationAllowsInputButCannotConfirmReplicaEvidence() throws Exception {
        publisher.observe(0, null); var response = publisher.send(EVENT).toCompletableFuture();
        var raw = a.next(); a.completeNext(); receipt(0, ack(raw, EVENT, 0, 4)); expire();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
    }
    @Test void directAcksWithoutIndependentNativeIdentitiesCannotClaimTwoStores() throws Exception {
        publisher.observe(1, new AcknowledgementReader.Generation(A, B));
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); fallback(); b.next();
        a.completeNext(); b.completeNext();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        assertTrue(response.join().reason().contains("distinct native log identities"));
    }
    @Test void metadataBlockingCannotBlockFallbackOrStartAnExpiredWrite() throws Exception {
        a.blockMetadata = true; var response = publisher.send(EVENT).toCompletableFuture();
        assertTrue(a.metadataEntered.await(2, TimeUnit.SECONDS)); fallback(); b.next(); b.completeNext(); expire();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        a.unblock.countDown(); assertTrue(a.metadataReturned.await(2, TimeUnit.SECONDS)); publisher.close();
        assertTrue(a.history().isEmpty());
    }
    @Test void topologyFailureStillAllowsOtherSide() throws Exception {
        a.wrongTopology = true; var response = publisher.send(EVENT).toCompletableFuture();
        b.next(); b.completeNext(); expire(); assertTrue(a.history().isEmpty());
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        assertEquals("IllegalStateException", response.join().clusterA().errorType());
    }
    @Test void synchronousSendFailureCannotBlockFallback() throws Exception {
        a.sendException = new IllegalStateException("broken client");
        var response = publisher.send(EVENT).toCompletableFuture(); b.next(); b.completeNext(); expire();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        assertTrue(a.history().isEmpty());
    }
    @Test void closeResolvesPendingConservativelyAndPrewriteRejectionsSendNothing() throws Exception {
        assertEquals(REJECTED, publisher.send(new Event("large", "key", new byte[128])).toCompletableFuture().join().status());
        assertTrue(a.history().isEmpty()); assertTrue(b.history().isEmpty());
        var response = publisher.send(EVENT).toCompletableFuture(); a.next(); a.completeNext(); publisher.close();
        assertEquals(ACCEPTED_DEGRADED, response.get(2, TimeUnit.SECONDS).status());
        assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status());
        assertTrue(a.closed()); assertTrue(b.closed()); assertEquals(0, publisher.pendingRequests());
    }
    @Test void callerCancellationCannotReleaseSdkCapacity() throws Exception {
        var response = publisher.send(EVENT).toCompletableFuture(); var raw = a.next(); response.cancel(false);
        assertEquals(1, publisher.pendingRequests()); assertEquals(REJECTED, publisher.send(EVENT).toCompletableFuture().join().status());
        a.completeNext(); receipt(0, ack(raw, EVENT, 0, 1)); assertEquals(0, publisher.pendingRequests());
    }
    @Test void nativeConfigCannotWeakenAcknowledgementsOrModifyRecords() {
        var supplied = Map.<String,Object>of("bootstrap.servers", "a:9092", "acks", "0", "enable.idempotence", false, "sasl.mechanism", "PLAIN");
        var effective = RoutedKafkaPublisher.producerProperties(PublishConfig.defaults("events"), supplied);
        assertEquals("all", effective.get("acks")); assertEquals(true, effective.get("enable.idempotence"));
        assertEquals("PLAIN", effective.get("sasl.mechanism")); assertEquals("0", supplied.get("acks"));
        for (String key : List.of("transactional.id", "interceptor.classes", "partitioner.class")) {
            assertThrows(IllegalArgumentException.class, () -> RoutedKafkaPublisher.producerProperties(
                    PublishConfig.defaults("events"), Map.of("bootstrap.servers", "a:9092", key, "custom")));
        }
    }
    @Test void concurrentAdmissionRejectsBeforeEitherRawWrite() throws Exception {
        var left = new ControlledProducer(); var right = new ControlledProducer();
        try (var bounded = new RoutedKafkaPublisher(new PublishConfig("events", TIMEOUT, 4, 128, Duration.ofMillis(200)),
                left, right, clock::get); var callers = Executors.newVirtualThreadPerTaskExecutor()) {
            var start = new CountDownLatch(1); var tasks = new java.util.ArrayList<Future<CompletableFuture<PublishResult>>>();
            for (int i=0;i<40;i++) { int id=i; tasks.add(callers.submit(() -> { start.await();
                return bounded.send(new Event("id-"+id, "kafka", new byte[]{1})).toCompletableFuture(); })); }
            start.countDown(); var responses = new java.util.ArrayList<CompletableFuture<PublishResult>>();
            for (var task : tasks) { responses.add(task.get(2, TimeUnit.SECONDS)); }
            assertEquals(4, bounded.pendingRequests());
            for (int i=0;i<4;i++) { left.next(); left.completeNext(); }
            assertTrue(right.history().isEmpty()); clock.set(TIMEOUT.toNanos()); bounded.expirePending();
            assertEquals(4, responses.stream().map(CompletableFuture::join).filter(r -> r.status()==ACCEPTED_DEGRADED).count());
            assertEquals(36, responses.stream().map(CompletableFuture::join).filter(r -> r.status()==REJECTED).count());
        }
    }
    private void fallback() { clock.set(Duration.ofSeconds(1).toNanos()); publisher.expirePending(); }
    private void expire() { clock.set(TIMEOUT.toNanos()); publisher.expirePending(); }
    private static ReplicationAck ack(ProducerRecord<byte[],byte[]> raw, Event event, long from, long to) {
        var bytes = ByteBuffer.wrap(raw.headers().lastHeader(EventCodec.ATTEMPT_HEADER).value());
        var id = new UUID(bytes.getLong(), bytes.getLong());
        boolean a = event.partition()==0;
        return new ReplicationAck(id, event.eventId(), EventCodec.hash(event.key(),event.payload()), event.partition(), a?A:B, from, a?B:A, to);
    }
    private void receipt(int side, ReplicationAck ack) {
        var record = ack.record("ack");
        publisher.receipt(side, new ConsumerRecord<>(record.topic(), record.partition(), 0, record.key(), record.value()), side==0?AB:BA);
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

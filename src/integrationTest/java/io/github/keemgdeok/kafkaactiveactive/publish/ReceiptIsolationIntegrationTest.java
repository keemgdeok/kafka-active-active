package io.github.keemgdeok.kafkaactiveactive.publish;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.internal.KafkaAccess;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationAck;
import io.github.keemgdeok.kafkaactiveactive.replication.ReplicationConfig;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.NativeLog;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;

class ReceiptIsolationIntegrationTest {
    private static final KafkaPair PAIR = new KafkaPair("receipt-isolation-brokers");
    @BeforeAll static void start() { PAIR.start(); }
    @AfterAll static void stop() throws Exception { PAIR.close(); }
    @Test void invalidUncommittedAndPastAttemptReceiptsCannotCreateConfirmation() throws Exception {
        String topic = "receipt-isolation-" + UUID.randomUUID(); PAIR.createTopic(topic);
        var config = ReplicationConfig.defaults(topic);
        try (var a = new KafkaAccess(PAIR.aProperties(), Duration.ofSeconds(5));
             var b = new KafkaAccess(PAIR.bProperties(), Duration.ofSeconds(5))) {
            a.confirmationTopic(config, KafkaAccess.deadline(Duration.ofSeconds(5)));
            b.confirmationTopic(config, KafkaAccess.deadline(Duration.ofSeconds(5)));
            var from = a.identity(topic, KafkaAccess.deadline(Duration.ofSeconds(5)), true);
            var to = b.identity(topic, KafkaAccess.deadline(Duration.ofSeconds(5)), true);
            var event = new Event("receipt-event", "kafka", new byte[]{1,2});
            var settings = new PublishConfig(topic, Duration.ofSeconds(8), 4, 1_024, Duration.ofSeconds(5));
            try (var publisher = ReplicationFixture.ready(RoutedKafkaPublisher.open(settings, PAIR.aProperties(), PAIR.bProperties()))) {
                var response = publisher.send(event).toCompletableFuture();
                long end = System.nanoTime() + Duration.ofSeconds(3).toNanos();
                List<org.apache.kafka.clients.consumer.ConsumerRecord<byte[],byte[]>> raw;
                do { raw = NativeLog.read(PAIR.aProperties(), topic); } while (raw.isEmpty() && System.nanoTime()<end);
                assertEquals(1,raw.size()); var record = raw.getFirst();
                var valid = new ReplicationAck(EventCodec.attempt(record), event.eventId(), EventCodec.hash(event.key(),event.payload()),
                        0,from,record.offset(),to,0);
                var oldFrom = new io.github.keemgdeok.kafkaactiveactive.replication.LogIdentity(from.clusterId(),"old-source");
                var oldTo = new io.github.keemgdeok.kafkaactiveactive.replication.LogIdentity(to.clusterId(),"old-target");
                for (var invalid : List.of(
                        new ReplicationAck(valid.attempt(),"wrong-id",valid.hash(),0,from,record.offset(),to,0),
                        new ReplicationAck(valid.attempt(),event.eventId(),new byte[32],0,from,record.offset(),to,0),
                        new ReplicationAck(valid.attempt(),event.eventId(),valid.hash(),1,from,record.offset(),to,0),
                        new ReplicationAck(valid.attempt(),event.eventId(),valid.hash(),0,from,record.offset()+1,to,0),
                        new ReplicationAck(valid.attempt(),event.eventId(),valid.hash(),0,oldFrom,record.offset(),to,0),
                        new ReplicationAck(valid.attempt(),event.eventId(),valid.hash(),0,from,record.offset(),oldTo,0))) {
                    NativeLog.write(PAIR.aProperties(), invalid.record(config.confirmationTopic()));
                }
                NativeLog.write(PAIR.aProperties(), new ProducerRecord<>(config.confirmationTopic(),0,
                        EventCodec.uuid(valid.attempt()),new byte[]{99}));
                end = System.nanoTime()+Duration.ofSeconds(3).toNanos();
                while (publisher.status().invalidAcknowledgements()<7 && System.nanoTime()<end) {
                    LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
                }
                assertEquals(7,publisher.status().invalidAcknowledgements()); assertFalse(response.isDone());
                var props = KafkaAccess.writer(PAIR.aProperties(), Duration.ofSeconds(5),1_024);
                props.put("transactional.id","receipt-test-"+UUID.randomUUID());
                var transaction = new KafkaProducer<byte[],byte[]>(props);
                try {
                    transaction.initTransactions(); transaction.beginTransaction();
                    transaction.send(valid.record(config.confirmationTopic())).get(5,TimeUnit.SECONDS);
                    // Keep it OPEN for the whole publish deadline, not a race against abort visibility.
                    var result = response.get(10,TimeUnit.SECONDS);
                    assertEquals(PublishResult.Status.ACCEPTED_DEGRADED,result.status());
                    assertTrue(NativeLog.read(PAIR.bProperties(),topic).isEmpty(),"no worker or fallback RAW on receipt lag");
                    transaction.abortTransaction();
                } finally { transaction.close(Duration.ZERO); }
                // Make a genuine peer copy, then a committed late receipt. Neither changes prior history
                // nor supplies evidence for the next RAW attempt with the same business ID.
                NativeLog.write(PAIR.bProperties(),EventCodec.replica(topic,record,1_024));
                NativeLog.write(PAIR.aProperties(),valid.record(config.confirmationTopic()));
                assertEquals(PublishResult.Status.ACCEPTED_DEGRADED,response.join().status());
                var retried = publisher.send(event).toCompletableFuture().get(10,TimeUnit.SECONDS);
                assertEquals(PublishResult.Status.ACCEPTED_DEGRADED,retried.status());
                assertEquals(2,NativeLog.read(PAIR.aProperties(),topic).size());
                assertEquals(1,NativeLog.read(PAIR.bProperties(),topic).size());
                assertEquals(8,NativeLog.read(PAIR.aProperties(),config.confirmationTopic()).size(),"aborted receipt invisible");
            }
        }
    }
}

package io.github.keemgdeok.kafkaactiveactive.consume;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import io.github.keemgdeok.kafkaactiveactive.internal.EventCodec;
import io.github.keemgdeok.kafkaactiveactive.publish.Event;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishConfig;
import io.github.keemgdeok.kafkaactiveactive.publish.PublishResult;
import io.github.keemgdeok.kafkaactiveactive.publish.RoutedKafkaPublisher;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairConfig;
import io.github.keemgdeok.kafkaactiveactive.repair.RepairService;
import io.github.keemgdeok.kafkaactiveactive.support.KafkaPair;
import io.github.keemgdeok.kafkaactiveactive.support.NativeLog;
import io.github.keemgdeok.kafkaactiveactive.support.ReplicationFixture;
import io.github.keemgdeok.kafkaactiveactive.verification.EventAudit;
import io.github.keemgdeok.kafkaactiveactive.verification.JsonlEvidence;

class LegacyUpgradeIntegrationTest {
    @Test void stoppedUpgradeRetainsLegacyDataCompletionIdsAndInputOffsetsAcrossV2Publishing() throws Exception {
        try (var pair=new KafkaPair("legacy-upgrade-brokers");
             var evidence=new JsonlEvidence(Path.of("build/reports/integrationTest/upgrade/events.jsonl"))) {
            pair.start(); String topic="upgrade-"+UUID.randomUUID(); pair.createTopic(topic);
            var settings=ConsumerSettings.defaults(topic,"upgrade-group-"+UUID.randomUUID());
            var done=new Event("already-completed","kafka",new byte[]{1});
            var unfinished=new Event("legacy-unfinished","hello",new byte[]{2});
            var oneSided=new Event("legacy-single-copy","kafka",new byte[]{3});
            for(var props:List.of(pair.aProperties(),pair.bProperties())) { NativeLog.write(props,EventCodec.encode(topic,done)); }
            try(var old=new ActiveKafkaConsumer(settings,pair.aProperties(),pair.bProperties())) {
                var batch=batch(old); assertEquals(List.of(done),batch.records().stream().map(Batch.Delivery::event).toList());
                assertEquals(CommitResult.Status.CONFIRMED,old.commit(batch.processed()).toCompletableFuture().join().status());
            }
            for(var props:List.of(pair.aProperties(),pair.bProperties())) { NativeLog.write(props,EventCodec.encode(topic,unfinished)); }
            NativeLog.write(pair.aProperties(),EventCodec.encode(topic,oneSided));
            String completionA; String completionB;
            try(var a=new CompletionLedger(settings,pair.aProperties());var b=new CompletionLedger(settings,pair.bProperties())) {
                completionA=a.snapshot().topicId(); completionB=b.snapshot().topicId();
            }
            var oldOffset=new TopicPartition(topic,0);
            org.apache.kafka.clients.consumer.OffsetAndMetadata offsetBefore;
            try(var admin=Admin.create(pair.aProperties())) {
                offsetBefore=admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata().get(5,TimeUnit.SECONDS).get(oldOffset);
                assertEquals(1,offsetBefore.offset());
            }
            try(var repair=new RepairService(RepairConfig.defaults(topic),pair.aProperties(),pair.bProperties())) {
                var report=repair.runOnce(); assertEquals(1,report.copiedToB());
            }
            assertEquals(1,new CompletionReplicator(settings,pair.aProperties(),pair.bProperties()).runOnce().completedIds());
            var fresh=List.of(new Event("v2-p0","kafka",new byte[]{4}),new Event("v2-p1","hello",new byte[]{5}));
            try(var replication=ReplicationFixture.start(topic,pair.aProperties(),pair.bProperties())) {
                long end=System.nanoTime()+Duration.ofSeconds(15).toNanos();
                var boundary=Map.of(0,2L,1,1L);
                while(System.nanoTime()<end && (!replication.status().aToB().committedOffsets().equals(boundary)
                        || !replication.status().bToA().committedOffsets().equals(boundary))) {
                    LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
                }
                assertEquals(boundary,replication.status().aToB().committedOffsets());
                assertEquals(boundary,replication.status().bToA().committedOffsets());
                assertEquals(0,replication.status().aToB().replicatedRecords()+replication.status().bToA().replicatedRecords());
                try(var admin=Admin.create(pair.aProperties())) {
                    assertEquals(offsetBefore,admin.listConsumerGroupOffsets(settings.groupId()).partitionsToOffsetAndMetadata()
                            .get(5,TimeUnit.SECONDS).get(oldOffset),"upgrade reset application input offsets");
                }
                try(var publisher=ReplicationFixture.ready(RoutedKafkaPublisher.open(PublishConfig.defaults(topic),pair.aProperties(),pair.bProperties()))) {
                    for(var event:List.of(done,fresh.get(0),fresh.get(1))) {
                        assertEquals(PublishResult.Status.CONFIRMED,publisher.send(event).toCompletableFuture().get(10,TimeUnit.SECONDS).status());
                    }
                }
                Set<String> calls=new HashSet<>();
                try(var consumer=new ActiveKafkaConsumer(settings,pair.aProperties(),pair.bProperties())) {
                    end=System.nanoTime()+Duration.ofSeconds(20).toNanos();
                    while(consumer.status().completedIds()<5 && System.nanoTime()<end) {
                        var batch=consumer.poll(Duration.ofMillis(100));
                        for(var delivery:batch.records()) {
                            assertNotEquals(done.eventId(),delivery.event().eventId(),"legacy completion lost after v2 retry");
                            assertTrue(calls.add(delivery.event().eventId()),"completed ID reached handler twice");
                        }
                        if(!batch.isEmpty()) { assertEquals(CommitResult.Status.CONFIRMED,consumer.commit(batch.processed()).toCompletableFuture().join().status()); }
                    }
                    assertEquals(5,consumer.status().completedIds());
                }
                assertEquals(Set.of(unfinished.eventId(),oneSided.eventId(),fresh.get(0).eventId(),fresh.get(1).eventId()),calls);
                evidence.record("delivery",Map.of("new_handler_ids",calls,"legacy_completed_handler_calls",0));
            }
            var expected=List.of(done,unfinished,oneSided,fresh.get(0),fresh.get(1));
            for(var props:List.of(pair.aProperties(),pair.bProperties())) {
                EventAudit.read(props,List.of(topic),expected).requireComplete();
                var records=NativeLog.read(props,topic);
                assertEquals(props.equals(pair.aProperties())?3:2,
                        records.stream().filter(r -> EventCodec.kind(r)==EventCodec.Kind.LEGACY).count(),"original v1 records were removed");
                try(var ledger=new CompletionLedger(settings,props)) {
                    assertEquals(props.equals(pair.aProperties())?completionA:completionB,ledger.snapshot().topicId());
                    assertEquals(5,ledger.replay(ledger.snapshot()).size());
                }
            }
            try(var repair=new RepairService(RepairConfig.defaults(topic),pair.aProperties(),pair.bProperties())) {
                var report=repair.runOnce(); assertEquals(0,report.copiedToA()+report.copiedToB());
            }
            evidence.record("verified",Map.of("preserved_completion_topics",true,"application_offset_preserved",true,
                    "completed_redeliveries",0,"data_and_completion_ids_both",5,"missing",0));
        }
    }
    private static Batch batch(ActiveKafkaConsumer consumer) {
        long end=System.nanoTime()+Duration.ofSeconds(20).toNanos();
        while(System.nanoTime()<end) { var batch=consumer.poll(Duration.ofMillis(100)); if(!batch.isEmpty()) { return batch; } }
        throw new AssertionError("upgrade batch unavailable: "+consumer.status());
    }
}

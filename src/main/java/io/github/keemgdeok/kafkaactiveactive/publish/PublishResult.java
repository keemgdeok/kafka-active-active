package io.github.keemgdeok.kafkaactiveactive.publish;

import java.util.Objects;

/** Immutable evidence at response time, not a claim about subsequent repair or late ACKs. */
public record PublishResult(String eventId, int partition, Status status,
                            BrokerResult clusterA, BrokerResult clusterB, String reason) {
    public enum Status { CONFIRMED, ACCEPTED_DEGRADED, UNKNOWN, REJECTED }
    public enum Delivery { ACKNOWLEDGED, FAILED, UNCONFIRMED }
    public enum Evidence { NONE, DIRECT_RAW_ACK, REPLICATION_ACK }

    public record BrokerResult(Delivery delivery, Long offset, String errorType, Evidence evidence) {
        public BrokerResult(Delivery delivery, Long offset, String errorType) {
            this(delivery, offset, errorType, delivery == Delivery.ACKNOWLEDGED
                    ? Evidence.DIRECT_RAW_ACK : Evidence.NONE);
        }
        public BrokerResult {
            Objects.requireNonNull(delivery, "delivery");
            Objects.requireNonNull(evidence, "evidence");
            if ((delivery == Delivery.ACKNOWLEDGED) != (evidence != Evidence.NONE)) {
                throw new IllegalArgumentException("acknowledged deliveries require ACK evidence");
            }
            if ((delivery == Delivery.ACKNOWLEDGED) != (offset != null)) {
                throw new IllegalArgumentException("only an ACK may carry an offset");
            }
            if (offset != null && offset < 0) {
                throw new IllegalArgumentException("ACK offset must be nonnegative");
            }
        }

        static BrokerResult unconfirmed() {
            return new BrokerResult(Delivery.UNCONFIRMED, null, null);
        }
    }

    public boolean successful() {
        return status == Status.CONFIRMED || status == Status.ACCEPTED_DEGRADED;
    }
}

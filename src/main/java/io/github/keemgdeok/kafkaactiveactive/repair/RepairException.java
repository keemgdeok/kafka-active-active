package io.github.keemgdeok.kafkaactiveactive.repair;

/** A failed run can have acknowledged partial copies; rerun from a fresh log snapshot. */
public final class RepairException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public enum Reason { CONFLICT, INDEX_LIMIT, INVALID_RECORD, INVALID_TOPOLOGY,
                         LOG_CHANGED, DEADLINE, CLOSED, BUSY, CLIENT_FAILURE }
    private final Reason reason;

    public RepairException(Reason reason, String message) {
        this(reason, message, null);
    }

    public RepairException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }
}

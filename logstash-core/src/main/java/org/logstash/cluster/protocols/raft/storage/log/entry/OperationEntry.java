package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Stores a state machine operation.
 * <p>
 * Each state machine operation is stored with a client-provided {@link #sequenceNumber() sequence number}.
 * The sequence number is used by state machines to apply client operations in the order in which they
 * were submitted by the client (FIFO order). Additionally, each operation is written with the leader's
 * {@link #timestamp() timestamp} at the time the entry was logged. This gives state machines an
 * approximation of time with which to react to the application of operations to the state machine.
 */
public abstract class OperationEntry extends SessionEntry {
    protected final long sequence;
    protected final RaftOperation operation;

    public OperationEntry(long term, long timestamp, long session, long sequence, RaftOperation operation) {
        super(term, timestamp, session);
        this.sequence = sequence;
        this.operation = operation;
    }

    /**
     * Returns the entry operation.
     * @return The entry operation.
     */
    public RaftOperation operation() {
        return operation;
    }

    /**
     * Returns the operation sequence number.
     * @return The operation sequence number.
     */
    public long sequenceNumber() {
        return sequence;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .add("session", session)
            .add("sequence", sequence)
            .add("operation", operation)
            .toString();
    }
}

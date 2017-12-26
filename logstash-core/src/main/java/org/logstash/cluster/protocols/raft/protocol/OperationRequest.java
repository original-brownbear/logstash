package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.Preconditions;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;

/**
 * Client operation request.
 * <p>
 * Operation requests are sent by clients to servers to execute operations on the replicated state
 * machine. Each operation request must be sequenced with a {@link #sequenceNumber()} number. All operations
 * will be applied to replicated state machines in the sequence in which they were sent by the client.
 * Sequence numbers must always be sequential, and in the event that an operation request fails, it must
 * be resent by the client.
 */
public abstract class OperationRequest extends SessionRequest {
    protected final long sequence;
    protected final RaftOperation operation;

    protected OperationRequest(long session, long sequence, RaftOperation operation) {
        super(session);
        this.sequence = sequence;
        this.operation = operation;
    }

    /**
     * Returns the request sequence number.
     * @return The request sequence number.
     */
    public long sequenceNumber() {
        return sequence;
    }

    /**
     * Returns the operation.
     * @return The operation.
     */
    public RaftOperation operation() {
        return operation;
    }

    /**
     * Operation request builder.
     */
    public abstract static class Builder<T extends OperationRequest.Builder<T, U>, U extends OperationRequest> extends SessionRequest.Builder<T, U> {
        protected long sequence;
        protected RaftOperation operation;

        /**
         * Sets the request sequence number.
         * @param sequence The request sequence number.
         * @return The request builder.
         * @throws IllegalArgumentException If the request sequence number is not positive.
         */
        @SuppressWarnings("unchecked")
        public T withSequence(long sequence) {
            Preconditions.checkArgument(sequence >= 0, "sequence must be positive");
            this.sequence = sequence;
            return (T) this;
        }

        /**
         * Sets the request operation.
         * @param operation The operation.
         * @return The request builder.
         * @throws NullPointerException if the request {@code operation} is {@code null}
         */
        @SuppressWarnings("unchecked")
        public T withOperation(RaftOperation operation) {
            this.operation = Preconditions.checkNotNull(operation, "operation cannot be null");
            return (T) this;
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkArgument(sequence >= 0, "sequence must be positive");
            Preconditions.checkNotNull(operation, "operation cannot be null");
        }
    }
}

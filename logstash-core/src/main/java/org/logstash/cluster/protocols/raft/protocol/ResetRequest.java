package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 * Event reset request.
 * <p>
 * Reset requests are sent by clients to servers if the client receives an event message out of
 * sequence to force the server to resend events from the correct index.
 */
public class ResetRequest extends SessionRequest {

    private final long index;

    public ResetRequest(long session, long index) {
        super(session);
        this.index = index;
    }

    /**
     * Returns a new publish response builder.
     * @return A new publish response builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the event index.
     * @return The event index.
     */
    public long index() {
        return index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), session, index);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof ResetRequest) {
            ResetRequest request = (ResetRequest) object;
            return request.session == session
                && request.index == index;
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("session", session)
            .add("index", index)
            .toString();
    }

    /**
     * Reset request builder.
     */
    public static class Builder extends SessionRequest.Builder<Builder, ResetRequest> {
        private long index;

        /**
         * Sets the event index.
         * @param index The event index.
         * @return The response builder.
         * @throws IllegalArgumentException if {@code index} is less than {@code 1}
         */
        public Builder withIndex(long index) {
            Preconditions.checkArgument(index >= 0, "index must be positive");
            this.index = index;
            return this;
        }

        /**
         * @throws IllegalStateException if sequence is less than 1
         */
        @Override
        public ResetRequest build() {
            validate();
            return new ResetRequest(session, index);
        }
    }
}

package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Server append entries response.
 */
public class AppendResponse extends AbstractRaftResponse {

    private final long term;
    private final boolean succeeded;
    private final long lastLogIndex;

    public AppendResponse(RaftResponse.Status status, RaftError error, long term, boolean succeeded, long lastLogIndex) {
        super(status, error);
        this.term = term;
        this.succeeded = succeeded;
        this.lastLogIndex = lastLogIndex;
    }

    /**
     * Returns a new append response builder.
     * @return A new append response builder.
     */
    public static AppendResponse.Builder builder() {
        return new AppendResponse.Builder();
    }

    /**
     * Returns the requesting node's current term.
     * @return The requesting node's current term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns a boolean indicating whether the append was successful.
     * @return Indicates whether the append was successful.
     */
    public boolean succeeded() {
        return succeeded;
    }

    /**
     * Returns the last index of the replica's log.
     * @return The last index of the responding replica's log.
     */
    public long lastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), status, term, succeeded, lastLogIndex);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof AppendResponse) {
            AppendResponse response = (AppendResponse) object;
            return response.status == status
                && response.term == term
                && response.succeeded == succeeded
                && response.lastLogIndex == lastLogIndex;
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == RaftResponse.Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("term", term)
                .add("succeeded", succeeded)
                .add("lastLogIndex", lastLogIndex)
                .toString();
        } else {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }

    /**
     * Append response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<AppendResponse.Builder, AppendResponse> {
        private long term;
        private boolean succeeded;
        private long lastLogIndex;

        /**
         * Sets the response term.
         * @param term The response term.
         * @return The append response builder
         * @throws IllegalArgumentException if {@code term} is not positive
         */
        public AppendResponse.Builder withTerm(long term) {
            Preconditions.checkArgument(term > 0, "term must be positive");
            this.term = term;
            return this;
        }

        /**
         * Sets whether the request succeeded.
         * @param succeeded Whether the append request succeeded.
         * @return The append response builder.
         */
        public AppendResponse.Builder withSucceeded(boolean succeeded) {
            this.succeeded = succeeded;
            return this;
        }

        /**
         * Sets the last index of the replica's log.
         * @param lastLogIndex The last index of the replica's log.
         * @return The append response builder.
         * @throws IllegalArgumentException if {@code index} is negative
         */
        public AppendResponse.Builder withLastLogIndex(long lastLogIndex) {
            Preconditions.checkArgument(lastLogIndex >= 0, "lastLogIndex must be positive");
            this.lastLogIndex = lastLogIndex;
            return this;
        }

        /**
         * @throws IllegalStateException if status is ok and term is not positive or log index is negative
         */
        @Override
        public AppendResponse build() {
            validate();
            return new AppendResponse(status, error, term, succeeded, lastLogIndex);
        }

        @Override
        protected void validate() {
            super.validate();
            if (status == RaftResponse.Status.OK) {
                Preconditions.checkArgument(term > 0, "term must be positive");
                Preconditions.checkArgument(lastLogIndex >= 0, "lastLogIndex must be positive");
            }
        }
    }
}

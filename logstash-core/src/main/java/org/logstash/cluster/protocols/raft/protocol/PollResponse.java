package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Server poll response.
 * <p>
 * Poll responses are sent by active servers in response to poll requests by followers to indicate
 * whether the responding server would vote for the requesting server if it were a candidate. This is
 * indicated by the {@link #accepted()} field of the response.
 */
public class PollResponse extends AbstractRaftResponse {

    private final long term;
    private final boolean accepted;

    public PollResponse(Status status, RaftError error, long term, boolean accepted) {
        super(status, error);
        this.term = term;
        this.accepted = accepted;
    }

    /**
     * Returns a new poll response builder.
     * @return A new poll response builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the responding node's current term.
     * @return The responding node's current term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns a boolean indicating whether the poll was accepted.
     * @return Indicates whether the poll was accepted.
     */
    public boolean accepted() {
        return accepted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), status, term, accepted);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof PollResponse) {
            PollResponse response = (PollResponse) object;
            return response.status == status
                && response.term == term
                && response.accepted == accepted;
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("term", term)
                .add("accepted", accepted)
                .toString();
        } else {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }

    /**
     * Poll response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<Builder, PollResponse> {
        private long term = -1;
        private boolean accepted;

        /**
         * Sets the response term.
         * @param term The response term.
         * @return The poll response builder.
         * @throws IllegalArgumentException if {@code term} is not positive
         */
        public Builder withTerm(long term) {
            Preconditions.checkArgument(term >= 0, "term must be positive");
            this.term = term;
            return this;
        }

        /**
         * Sets whether the poll was granted.
         * @param accepted Whether the poll was granted.
         * @return The poll response builder.
         */
        public Builder withAccepted(boolean accepted) {
            this.accepted = accepted;
            return this;
        }

        @Override
        public PollResponse build() {
            validate();
            return new PollResponse(status, error, term, accepted);
        }

        @Override
        protected void validate() {
            super.validate();
            if (status == Status.OK) {
                Preconditions.checkArgument(term >= 0, "term must be positive");
            }
        }
    }
}

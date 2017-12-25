package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Server vote response.
 * <p>
 * Vote responses are sent by active servers in response to vote requests by candidate to indicate
 * whether the responding server voted for the requesting candidate. This is indicated by the
 * {@link #voted()} field of the response.
 */
public class VoteResponse extends AbstractRaftResponse {

    private final long term;
    private final boolean voted;

    public VoteResponse(Status status, RaftError error, long term, boolean voted) {
        super(status, error);
        this.term = term;
        this.voted = voted;
    }

    /**
     * Returns a new vote response builder.
     * @return A new vote response builder.
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
     * Returns a boolean indicating whether the vote was granted.
     * @return Indicates whether the vote was granted.
     */
    public boolean voted() {
        return voted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), status, term, voted);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof VoteResponse) {
            VoteResponse response = (VoteResponse) object;
            return response.status == status
                && response.term == term
                && response.voted == voted;
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("term", term)
                .add("voted", voted)
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
    public static class Builder extends AbstractRaftResponse.Builder<Builder, VoteResponse> {
        private long term = -1;
        private boolean voted;

        /**
         * Sets the response term.
         * @param term The response term.
         * @return The vote response builder.
         * @throws IllegalArgumentException if {@code term} is negative
         */
        public Builder withTerm(long term) {
            Preconditions.checkArgument(term >= 0, "term must be positive");
            this.term = term;
            return this;
        }

        /**
         * Sets whether the vote was granted.
         * @param voted Whether the vote was granted.
         * @return The vote response builder.
         */
        public Builder withVoted(boolean voted) {
            this.voted = voted;
            return this;
        }

        @Override
        public VoteResponse build() {
            validate();
            return new VoteResponse(status, error, term, voted);
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

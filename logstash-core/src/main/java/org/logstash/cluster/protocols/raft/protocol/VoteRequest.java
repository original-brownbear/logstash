package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.MemberId;

/**
 * Server vote request.
 * <p>
 * Vote requests are sent by candidate servers during an election to determine whether they should
 * become the leader for a cluster. Vote requests contain the necessary information for followers to
 * determine whether a candidate should receive their vote based on log and other information.
 */
public class VoteRequest extends AbstractRaftRequest {

    private final long term;
    private final String candidate;
    private final long lastLogIndex;
    private final long lastLogTerm;

    public VoteRequest(long term, String candidate, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    /**
     * Returns a new vote request builder.
     * @return A new vote request builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the requesting node's current term.
     * @return The requesting node's current term.
     */
    public long term() {
        return term;
    }

    /**
     * Returns the candidate's address.
     * @return The candidate's address.
     */
    public MemberId candidate() {
        return MemberId.from(candidate);
    }

    /**
     * Returns the candidate's last log index.
     * @return The candidate's last log index.
     */
    public long lastLogIndex() {
        return lastLogIndex;
    }

    /**
     * Returns the candidate's last log term.
     * @return The candidate's last log term.
     */
    public long lastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), term, candidate, lastLogIndex, lastLogTerm);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof VoteRequest) {
            VoteRequest request = (VoteRequest) object;
            return request.term == term
                && request.candidate == candidate
                && request.lastLogIndex == lastLogIndex
                && request.lastLogTerm == lastLogTerm;
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("candidate", candidate)
            .add("lastLogIndex", lastLogIndex)
            .add("lastLogTerm", lastLogTerm)
            .toString();
    }

    /**
     * Vote request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<Builder, VoteRequest> {
        private long term = -1;
        private String candidate;
        private long lastLogIndex = -1;
        private long lastLogTerm = -1;

        /**
         * Sets the request term.
         * @param term The request term.
         * @return The poll request builder.
         * @throws IllegalArgumentException if {@code term} is negative
         */
        public Builder withTerm(long term) {
            Preconditions.checkArgument(term >= 0, "term must be positive");
            this.term = term;
            return this;
        }

        /**
         * Sets the request leader.
         * @param candidate The request candidate.
         * @return The poll request builder.
         * @throws IllegalArgumentException if {@code candidate} is not positive
         */
        public Builder withCandidate(MemberId candidate) {
            this.candidate = Preconditions.checkNotNull(candidate, "candidate cannot be null").id();
            return this;
        }

        /**
         * Sets the request last log index.
         * @param logIndex The request last log index.
         * @return The poll request builder.
         * @throws IllegalArgumentException if {@code index} is negative
         */
        public Builder withLastLogIndex(long logIndex) {
            Preconditions.checkArgument(logIndex >= 0, "lastLogIndex must be positive");
            this.lastLogIndex = logIndex;
            return this;
        }

        /**
         * Sets the request last log term.
         * @param logTerm The request last log term.
         * @return The poll request builder.
         * @throws IllegalArgumentException if {@code term} is negative
         */
        public Builder withLastLogTerm(long logTerm) {
            Preconditions.checkArgument(logTerm >= 0, "lastLogTerm must be positive");
            this.lastLogTerm = logTerm;
            return this;
        }

        @Override
        public VoteRequest build() {
            validate();
            return new VoteRequest(term, candidate, lastLogIndex, lastLogTerm);
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkArgument(term >= 0, "term must be positive");
            Preconditions.checkNotNull(candidate, "candidate cannot be null");
            Preconditions.checkArgument(lastLogIndex >= 0, "lastLogIndex must be positive");
            Preconditions.checkArgument(lastLogTerm >= 0, "lastLogTerm must be positive");
        }
    }
}

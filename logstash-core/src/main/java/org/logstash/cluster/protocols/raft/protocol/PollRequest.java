package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.MemberId;

/**
 * Server poll request.
 * <p>
 * Poll requests aid in the implementation of the so-called "pre-vote" protocol. They are sent by followers
 * to all other servers prior to transitioning to the candidate state. This helps ensure that servers that
 * can't win elections do not disrupt existing leaders when e.g. rejoining the cluster after a partition.
 */
public class PollRequest extends AbstractRaftRequest {

    private final long term;
    private final String candidate;
    private final long lastLogIndex;
    private final long lastLogTerm;

    public PollRequest(long term, String candidate, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    /**
     * Returns a new poll request builder.
     * @return A new poll request builder.
     */
    public static PollRequest.Builder builder() {
        return new PollRequest.Builder();
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
        if (object instanceof PollRequest) {
            PollRequest request = (PollRequest) object;
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
     * Poll request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<PollRequest.Builder, PollRequest> {
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
        public PollRequest.Builder withTerm(long term) {
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
        public PollRequest.Builder withCandidate(MemberId candidate) {
            this.candidate = Preconditions.checkNotNull(candidate, "candidate cannot be null").id();
            return this;
        }

        /**
         * Sets the request last log index.
         * @param logIndex The request last log index.
         * @return The poll request builder.
         * @throws IllegalArgumentException if {@code index} is negative
         */
        public PollRequest.Builder withLastLogIndex(long logIndex) {
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
        public PollRequest.Builder withLastLogTerm(long logTerm) {
            Preconditions.checkArgument(logTerm >= 0, "lastLogTerm must be positive");
            this.lastLogTerm = logTerm;
            return this;
        }

        /**
         * @throws IllegalStateException if candidate is not positive or if term, lastLogIndex or lastLogTerm are negative
         */
        @Override
        public PollRequest build() {
            validate();
            return new PollRequest(term, candidate, lastLogIndex, lastLogTerm);
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

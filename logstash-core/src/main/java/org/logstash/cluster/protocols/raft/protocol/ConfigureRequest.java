package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Configuration installation request.
 * <p>
 * Configuration requests are special requests that aid in installing committed configurations
 * to passive and reserve members of the cluster. Prior to the start of replication from an active
 * member to a passive or reserve member, the active member must update the passive/reserve member's
 * configuration to ensure it is in the expected state.
 */
public class ConfigureRequest extends AbstractRaftRequest {

    private final long term;
    private final String leader;
    private final long index;
    private final long timestamp;
    private final Collection<RaftMember> members;

    public ConfigureRequest(long term, String leader, long index, long timestamp, Collection<RaftMember> members) {
        this.term = term;
        this.leader = leader;
        this.index = index;
        this.timestamp = timestamp;
        this.members = members;
    }

    /**
     * Returns a new configuration request builder.
     * @return A new configuration request builder.
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
     * Returns the requesting leader address.
     * @return The leader's address.
     */
    public MemberId leader() {
        return MemberId.from(leader);
    }

    /**
     * Returns the configuration index.
     * @return The configuration index.
     */
    public long index() {
        return index;
    }

    /**
     * Returns the configuration timestamp.
     * @return The configuration timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * Returns the configuration members.
     * @return The configuration members.
     */
    public Collection<RaftMember> members() {
        return members;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), term, leader, index, members);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof ConfigureRequest) {
            ConfigureRequest request = (ConfigureRequest) object;
            return request.term == term
                && request.leader == leader
                && request.index == index
                && request.timestamp == timestamp
                && request.members.equals(members);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("leader", leader)
            .add("index", index)
            .add("timestamp", timestamp)
            .add("members", members)
            .toString();
    }

    /**
     * Heartbeat request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<Builder, ConfigureRequest> {
        private long term;
        private String leader;
        private long index;
        private long timestamp;
        private Collection<RaftMember> members;

        /**
         * Sets the request term.
         * @param term The request term.
         * @return The append request builder.
         * @throws IllegalArgumentException if the {@code term} is not positive
         */
        public Builder withTerm(long term) {
            Preconditions.checkArgument(term > 0, "term must be positive");
            this.term = term;
            return this;
        }

        /**
         * Sets the request leader.
         * @param leader The request leader.
         * @return The append request builder.
         * @throws IllegalArgumentException if the {@code leader} is not positive
         */
        public Builder withLeader(MemberId leader) {
            this.leader = Preconditions.checkNotNull(leader, "leader cannot be null").id();
            return this;
        }

        /**
         * Sets the request index.
         * @param index The request index.
         * @return The request builder.
         */
        public Builder withIndex(long index) {
            Preconditions.checkArgument(index >= 0, "index must be positive");
            this.index = index;
            return this;
        }

        /**
         * Sets the request timestamp.
         * @param timestamp The request timestamp.
         * @return The request builder.
         */
        public Builder withTime(long timestamp) {
            Preconditions.checkArgument(timestamp > 0, "timestamp must be positive");
            this.timestamp = timestamp;
            return this;
        }

        /**
         * Sets the request members.
         * @param members The request members.
         * @return The request builder.
         * @throws NullPointerException if {@code member} is null
         */
        public Builder withMembers(Collection<RaftMember> members) {
            this.members = Preconditions.checkNotNull(members, "members cannot be null");
            return this;
        }

        /**
         * @throws IllegalStateException if member is null
         */
        @Override
        public ConfigureRequest build() {
            validate();
            return new ConfigureRequest(term, leader, index, timestamp, members);
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkArgument(term > 0, "term must be positive");
            Preconditions.checkNotNull(leader, "leader cannot be null");
            Preconditions.checkArgument(index >= 0, "index must be positive");
            Preconditions.checkArgument(timestamp > 0, "timestamp must be positive");
            Preconditions.checkNotNull(members, "members cannot be null");
        }
    }

}

package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.MemberId;

/**
 * Client heartbeat request.
 */
public class HeartbeatRequest extends AbstractRaftRequest {

    private final MemberId leader;
    private final Collection<MemberId> members;

    public HeartbeatRequest(MemberId leader, Collection<MemberId> members) {
        this.leader = leader;
        this.members = members;
    }

    /**
     * Returns a new heartbeat request builder.
     * @return A new heartbeat request builder.
     */
    public static HeartbeatRequest.Builder builder() {
        return new HeartbeatRequest.Builder();
    }

    /**
     * Returns the cluster leader.
     * @return The cluster leader.
     */
    public MemberId leader() {
        return leader;
    }

    /**
     * Returns the cluster members.
     * @return The cluster members.
     */
    public Collection<MemberId> members() {
        return members;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), leader, members);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof HeartbeatRequest) {
            HeartbeatRequest request = (HeartbeatRequest) object;
            return Objects.equals(request.leader, leader) && Objects.equals(request.members, members);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("leader", leader)
            .add("members", members)
            .toString();
    }

    /**
     * Heartbeat request builder.
     */
    public static class Builder extends AbstractRaftRequest.Builder<HeartbeatRequest.Builder, HeartbeatRequest> {
        private MemberId leader;
        private Collection<MemberId> members;

        /**
         * Sets the request leader.
         * @param leader The request leader.
         * @return The request builder.
         */
        public HeartbeatRequest.Builder withLeader(MemberId leader) {
            this.leader = leader;
            return this;
        }

        /**
         * Sets the request members.
         * @param members The request members.
         * @return The request builder.
         * @throws NullPointerException if {@code members} is null
         */
        public HeartbeatRequest.Builder withMembers(Collection<MemberId> members) {
            this.members = Preconditions.checkNotNull(members, "members cannot be null");
            return this;
        }

        /**
         * @throws IllegalStateException if status is OK and members is null
         */
        @Override
        public HeartbeatRequest build() {
            validate();
            return new HeartbeatRequest(leader, members);
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkNotNull(members, "members cannot be null");
        }
    }
}

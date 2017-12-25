package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Session keep alive response.
 * <p>
 * Session keep alive responses are sent upon the completion of a {@link KeepAliveRequest}
 * from a client. Keep alive responses, when successful, provide the current cluster configuration and leader
 * to the client to ensure clients can evolve with the structure of the cluster and make intelligent decisions
 * about connecting to the cluster.
 */
public class KeepAliveResponse extends AbstractRaftResponse {

    private final MemberId leader;
    private final Collection<MemberId> members;
    private final long[] sessionIds;
    public KeepAliveResponse(Status status, RaftError error, MemberId leader, Collection<MemberId> members, long[] sessionIds) {
        super(status, error);
        this.leader = leader;
        this.members = members;
        this.sessionIds = sessionIds;
    }

    /**
     * Returns a new keep alive response builder.
     * @return A new keep alive response builder.
     */
    public static Builder builder() {
        return new Builder();
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

    /**
     * Returns the sessions that were successfully kept alive.
     * @return The sessions that were successfully kept alive.
     */
    public long[] sessionIds() {
        return sessionIds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), status, leader, members);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof KeepAliveResponse) {
            KeepAliveResponse response = (KeepAliveResponse) object;
            return response.status == status
                && ((response.leader == null && leader == null)
                || (response.leader != null && leader != null && response.leader.equals(leader)))
                && ((response.members == null && members == null)
                || (response.members != null && members != null && response.members.equals(members)));
        }
        return false;
    }

    @Override
    public String toString() {
        if (status == Status.OK) {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("leader", leader)
                .add("members", members)
                .add("sessionIds", ArraySizeHashPrinter.of(sessionIds))
                .toString();
        } else {
            return MoreObjects.toStringHelper(this)
                .add("status", status)
                .add("error", error)
                .toString();
        }
    }

    /**
     * Status response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<Builder, KeepAliveResponse> {
        private MemberId leader;
        private Collection<MemberId> members;
        private long[] sessionIds;

        /**
         * Sets the response leader.
         * @param leader The response leader.
         * @return The response builder.
         */
        public Builder withLeader(MemberId leader) {
            this.leader = leader;
            return this;
        }

        /**
         * Sets the response members.
         * @param members The response members.
         * @return The response builder.
         * @throws NullPointerException if {@code members} is null
         */
        public Builder withMembers(Collection<MemberId> members) {
            this.members = Preconditions.checkNotNull(members, "members cannot be null");
            return this;
        }

        /**
         * Sets the response sessions.
         * @param sessionIds the response sessions
         * @return the response builder
         */
        public Builder withSessionIds(long[] sessionIds) {
            this.sessionIds = Preconditions.checkNotNull(sessionIds, "sessionIds cannot be null");
            return this;
        }

        /**
         * @throws IllegalStateException if status is OK and members is null
         */
        @Override
        public KeepAliveResponse build() {
            validate();
            return new KeepAliveResponse(status, error, leader, members, sessionIds);
        }

        @Override
        protected void validate() {
            super.validate();
            if (status == Status.OK) {
                Preconditions.checkNotNull(members, "members cannot be null");
                Preconditions.checkNotNull(sessionIds, "sessionIds cannot be null");
            }
        }
    }
}

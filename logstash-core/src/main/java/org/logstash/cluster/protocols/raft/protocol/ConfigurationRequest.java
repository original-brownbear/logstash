package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Configuration change request.
 * <p>
 * Configuration change requests are the basis for members joining and leaving the cluster.
 * When a member wants to join or leave the cluster, it must submit a configuration change
 * request to the leader where the change will be logged and replicated.
 */
public abstract class ConfigurationRequest extends AbstractRaftRequest {
    protected final RaftMember member;

    protected ConfigurationRequest(RaftMember member) {
        this.member = member;
    }

    /**
     * Returns the member to configure.
     * @return The member to configure.
     */
    public RaftMember member() {
        return member;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), member);
    }

    @Override
    public boolean equals(Object object) {
        if (getClass().isAssignableFrom(object.getClass())) {
            return ((ConfigurationRequest) object).member.equals(member);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("member", member)
            .toString();
    }

    /**
     * Configuration request builder.
     */
    public abstract static class Builder<T extends ConfigurationRequest.Builder<T, U>, U extends ConfigurationRequest> extends AbstractRaftRequest.Builder<T, U> {
        protected RaftMember member;

        /**
         * Sets the request member.
         * @param member The request member.
         * @return The request builder.
         * @throws NullPointerException if {@code member} is null
         */
        @SuppressWarnings("unchecked")
        public T withMember(RaftMember member) {
            this.member = Preconditions.checkNotNull(member, "member cannot be null");
            return (T) this;
        }

        @Override
        protected void validate() {
            super.validate();
            Preconditions.checkNotNull(member, "member cannot be null");
        }
    }
}

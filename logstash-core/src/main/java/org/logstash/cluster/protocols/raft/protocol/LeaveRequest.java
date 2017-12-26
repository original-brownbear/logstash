package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Server leave configuration request.
 * <p>
 * The leave request is the mechanism by which servers remove themselves from a cluster. When a server
 * wants to leave a cluster, it must submit a leave request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 */
public class LeaveRequest extends ConfigurationRequest {

    public LeaveRequest(RaftMember member) {
        super(member);
    }

    /**
     * Returns a new leave request builder.
     * @return A new leave request builder.
     */
    public static LeaveRequest.Builder builder() {
        return new LeaveRequest.Builder();
    }

    /**
     * Leave request builder.
     */
    public static class Builder extends ConfigurationRequest.Builder<LeaveRequest.Builder, LeaveRequest> {
        @Override
        public LeaveRequest build() {
            validate();
            return new LeaveRequest(member);
        }
    }
}

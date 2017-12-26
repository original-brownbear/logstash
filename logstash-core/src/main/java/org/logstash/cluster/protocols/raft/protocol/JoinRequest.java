package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Server join configuration change request.
 * <p>
 * The join request is the mechanism by which new servers join a cluster. When a server wants to
 * join a cluster, it must submit a join request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 */
public class JoinRequest extends ConfigurationRequest {

    public JoinRequest(RaftMember member) {
        super(member);
    }

    /**
     * Returns a new join request builder.
     * @return A new join request builder.
     */
    public static JoinRequest.Builder builder() {
        return new JoinRequest.Builder();
    }

    /**
     * Join request builder.
     */
    public static class Builder extends ConfigurationRequest.Builder<JoinRequest.Builder, JoinRequest> {
        @Override
        public JoinRequest build() {
            validate();
            return new JoinRequest(member);
        }
    }
}

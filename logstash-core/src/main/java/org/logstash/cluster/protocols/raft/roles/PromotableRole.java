package org.logstash.cluster.protocols.raft.roles;

import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.impl.RaftContext;

/**
 * Promotable role.
 */
public class PromotableRole extends PassiveRole {
    public PromotableRole(RaftContext context) {
        super(context);
    }

    @Override
    public RaftServer.Role role() {
        return RaftServer.Role.PROMOTABLE;
    }
}

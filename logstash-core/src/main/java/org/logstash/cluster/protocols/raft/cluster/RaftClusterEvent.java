package org.logstash.cluster.protocols.raft.cluster;

import org.logstash.cluster.event.AbstractEvent;

/**
 * Raft cluster event.
 */
public class RaftClusterEvent extends AbstractEvent<RaftClusterEvent.Type, RaftMember> {

    public RaftClusterEvent(Type type, RaftMember subject) {
        super(type, subject);
    }

    public RaftClusterEvent(Type type, RaftMember subject, long time) {
        super(type, subject, time);
    }

    /**
     * Raft cluster event type.
     */
    public enum Type {
        JOIN,
        LEAVE,
    }
}

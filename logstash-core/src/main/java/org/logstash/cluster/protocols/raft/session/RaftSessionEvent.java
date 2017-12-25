package org.logstash.cluster.protocols.raft.session;

import org.logstash.cluster.event.AbstractEvent;

/**
 * Raft session event.
 */
public class RaftSessionEvent extends AbstractEvent<RaftSessionEvent.Type, RaftSession> {

    public RaftSessionEvent(RaftSessionEvent.Type type, RaftSession subject, long time) {
        super(type, subject, time);
    }

    /**
     * Raft session type.
     */
    public enum Type {
        /**
         * Indicates that a session has been opened by the client.
         */
        OPEN,

        /**
         * Indicates that a session has been expired by the cluster.
         */
        EXPIRE,

        /**
         * Indicates that a session has been closed by the client.
         */
        CLOSE,
    }
}

package org.logstash.cluster.protocols.raft.session;

import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Session identifier.
 */
public class SessionId extends AbstractIdentifier<Long> {

    protected SessionId() {
    }

    public SessionId(Long value) {
        super(value);
    }

    /**
     * Returns a new session ID from the given identifier.
     * @param id the identifier from which to create a session ID
     * @return a new session identifier
     */
    public static SessionId from(long id) {
        return new SessionId(id);
    }
}

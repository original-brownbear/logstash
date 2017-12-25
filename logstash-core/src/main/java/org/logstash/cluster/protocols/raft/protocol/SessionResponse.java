package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Base session response.
 */
public abstract class SessionResponse extends AbstractRaftResponse {
    protected SessionResponse(Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Session response builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends SessionResponse> extends AbstractRaftResponse.Builder<T, U> {
    }
}

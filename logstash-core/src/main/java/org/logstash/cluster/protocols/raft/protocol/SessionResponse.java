package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Base session response.
 */
public abstract class SessionResponse extends AbstractRaftResponse {
    protected SessionResponse(RaftResponse.Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Session response builder.
     */
    public abstract static class Builder<T extends SessionResponse.Builder<T, U>, U extends SessionResponse> extends AbstractRaftResponse.Builder<T, U> {
    }
}

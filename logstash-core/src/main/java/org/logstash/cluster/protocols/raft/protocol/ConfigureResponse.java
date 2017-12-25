package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Configuration installation response.
 */
public class ConfigureResponse extends AbstractRaftResponse {

    public ConfigureResponse(Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Returns a new configure response builder.
     * @return A new configure response builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Heartbeat response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<Builder, ConfigureResponse> {
        @Override
        public ConfigureResponse build() {
            return new ConfigureResponse(status, error);
        }
    }
}

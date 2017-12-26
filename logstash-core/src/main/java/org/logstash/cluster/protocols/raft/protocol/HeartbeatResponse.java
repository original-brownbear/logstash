package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Client heartbeat response.
 */
public class HeartbeatResponse extends AbstractRaftResponse {

    public HeartbeatResponse(RaftResponse.Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Returns a new heartbeat response builder.
     * @return A new heartbeat response builder.
     */
    public static HeartbeatResponse.Builder builder() {
        return new HeartbeatResponse.Builder();
    }

    /**
     * Heartbeat response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<HeartbeatResponse.Builder, HeartbeatResponse> {
        @Override
        public HeartbeatResponse build() {
            validate();
            return new HeartbeatResponse(status, error);
        }
    }
}

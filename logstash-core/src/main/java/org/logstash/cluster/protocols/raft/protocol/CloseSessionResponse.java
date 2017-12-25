package org.logstash.cluster.protocols.raft.protocol;

import java.util.Objects;
import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Close session response.
 */
public class CloseSessionResponse extends SessionResponse {

    public CloseSessionResponse(RaftResponse.Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Returns a new keep alive response builder.
     * @return A new keep alive response builder.
     */
    public static CloseSessionResponse.Builder builder() {
        return new CloseSessionResponse.Builder();
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof CloseSessionResponse) {
            CloseSessionResponse response = (CloseSessionResponse) object;
            return response.status == status && Objects.equals(response.error, error);
        }
        return false;
    }

    /**
     * Status response builder.
     */
    public static class Builder extends SessionResponse.Builder<CloseSessionResponse.Builder, CloseSessionResponse> {
        @Override
        public CloseSessionResponse build() {
            validate();
            return new CloseSessionResponse(status, error);
        }
    }
}

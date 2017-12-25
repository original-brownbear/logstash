package org.logstash.cluster.protocols.raft.protocol;

/**
 * Close session request.
 */
public class CloseSessionRequest extends SessionRequest {

    public CloseSessionRequest(long session) {
        super(session);
    }

    /**
     * Returns a new unregister request builder.
     * @return A new unregister request builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Unregister request builder.
     */
    public static class Builder extends SessionRequest.Builder<Builder, CloseSessionRequest> {
        @Override
        public CloseSessionRequest build() {
            validate();
            return new CloseSessionRequest(session);
        }
    }
}

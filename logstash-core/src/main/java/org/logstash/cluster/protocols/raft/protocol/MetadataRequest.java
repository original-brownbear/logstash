package org.logstash.cluster.protocols.raft.protocol;

/**
 * Cluster metadata request.
 */
public class MetadataRequest extends SessionRequest {

    public MetadataRequest(long session) {
        super(session);
    }

    /**
     * Returns a new metadata request builder.
     * @return A new metadata request builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Metadata request builder.
     */
    public static class Builder extends SessionRequest.Builder<Builder, MetadataRequest> {
        @Override
        public MetadataRequest build() {
            return new MetadataRequest(session);
        }
    }
}

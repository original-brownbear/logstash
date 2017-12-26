package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Leadership transfer response.
 */
public class TransferResponse extends AbstractRaftResponse {

    public TransferResponse(RaftResponse.Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Returns a new transfer response builder.
     * @return A new transfer response builder.
     */
    public static TransferResponse.Builder builder() {
        return new TransferResponse.Builder();
    }

    /**
     * Join response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<TransferResponse.Builder, TransferResponse> {
        @Override
        public TransferResponse build() {
            validate();
            return new TransferResponse(status, error);
        }
    }
}

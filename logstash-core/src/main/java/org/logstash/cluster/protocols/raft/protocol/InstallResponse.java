package org.logstash.cluster.protocols.raft.protocol;

import org.logstash.cluster.protocols.raft.RaftError;

/**
 * Snapshot installation response.
 * <p>
 * Install responses are sent once a snapshot installation request has been received and processed.
 * Install responses provide no additional metadata aside from indicating whether or not the request
 * was successful.
 */
public class InstallResponse extends AbstractRaftResponse {

    public InstallResponse(RaftResponse.Status status, RaftError error) {
        super(status, error);
    }

    /**
     * Returns a new install response builder.
     * @return A new install response builder.
     */
    public static InstallResponse.Builder builder() {
        return new InstallResponse.Builder();
    }

    /**
     * Install response builder.
     */
    public static class Builder extends AbstractRaftResponse.Builder<InstallResponse.Builder, InstallResponse> {
        @Override
        public InstallResponse build() {
            validate();
            return new InstallResponse(status, error);
        }
    }
}

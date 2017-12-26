package org.logstash.cluster.protocols.raft.protocol;

import java.util.Collection;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Server configuration change response.
 */
public class ReconfigureResponse extends ConfigurationResponse {

    public ReconfigureResponse(RaftResponse.Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
        super(status, error, index, term, timestamp, members);
    }

    /**
     * Returns a new reconfigure response builder.
     * @return A new reconfigure response builder.
     */
    public static ReconfigureResponse.Builder builder() {
        return new ReconfigureResponse.Builder();
    }

    /**
     * Reconfigure response builder.
     */
    public static class Builder extends ConfigurationResponse.Builder<ReconfigureResponse.Builder, ReconfigureResponse> {
        @Override
        public ReconfigureResponse build() {
            validate();
            return new ReconfigureResponse(status, error, index, term, timestamp, members);
        }
    }
}

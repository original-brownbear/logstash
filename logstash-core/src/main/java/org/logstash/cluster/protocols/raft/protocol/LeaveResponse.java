package org.logstash.cluster.protocols.raft.protocol;

import java.util.Collection;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;

/**
 * Server leave configuration change response.
 */
public class LeaveResponse extends ConfigurationResponse {

    public LeaveResponse(RaftResponse.Status status, RaftError error, long index, long term, long timestamp, Collection<RaftMember> members) {
        super(status, error, index, term, timestamp, members);
    }

    /**
     * Returns a new leave response builder.
     * @return A new leave response builder.
     */
    public static LeaveResponse.Builder builder() {
        return new LeaveResponse.Builder();
    }

    /**
     * Leave response builder.
     */
    public static class Builder extends ConfigurationResponse.Builder<LeaveResponse.Builder, LeaveResponse> {
        @Override
        public LeaveResponse build() {
            validate();
            return new LeaveResponse(status, error, index, term, timestamp, members);
        }
    }
}

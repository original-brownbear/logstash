package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.MoreObjects;
import java.util.Set;
import org.logstash.cluster.protocols.raft.session.RaftSessionMetadata;

/**
 * Metadata result.
 */
public final class MetadataResult {
    final Set<RaftSessionMetadata> sessions;

    MetadataResult(Set<RaftSessionMetadata> sessions) {
        this.sessions = sessions;
    }

    /**
     * Returns the session metadata.
     * @return The session metadata.
     */
    public Set<RaftSessionMetadata> sessions() {
        return sessions;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("sessions", sessions)
            .toString();
    }
}

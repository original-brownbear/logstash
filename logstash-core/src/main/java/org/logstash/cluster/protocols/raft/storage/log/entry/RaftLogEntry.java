package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;

/**
 * Stores a state change in a {@link org.logstash.cluster.protocols.raft.storage.log.RaftLog}.
 */
public abstract class RaftLogEntry {
    protected final long term;

    public RaftLogEntry(long term) {
        this.term = term;
    }

    /**
     * Returns the entry term.
     * @return The entry term.
     */
    public long term() {
        return term;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .toString();
    }
}

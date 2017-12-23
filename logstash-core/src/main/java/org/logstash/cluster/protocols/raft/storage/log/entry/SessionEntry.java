package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Base class for session-related entries.
 */
public abstract class SessionEntry extends TimestampedEntry {
    protected final long session;

    public SessionEntry(long term, long timestamp, long session) {
        super(term, timestamp);
        this.session = session;
    }

    /**
     * Returns the session ID.
     * @return The session ID.
     */
    public long session() {
        return session;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .add("session", session)
            .toString();
    }
}

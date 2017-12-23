package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Close session entry.
 */
public class CloseSessionEntry extends SessionEntry {
    private final boolean expired;

    public CloseSessionEntry(long term, long timestamp, long session, boolean expired) {
        super(term, timestamp, session);
        this.expired = expired;
    }

    /**
     * Returns whether the session is expired.
     * @return Indicates whether the session is expired.
     */
    public boolean expired() {
        return expired;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .add("session", session)
            .add("expired", expired)
            .toString();
    }
}

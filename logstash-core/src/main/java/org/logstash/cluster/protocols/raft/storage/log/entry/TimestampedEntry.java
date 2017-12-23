package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Base class for timestamped entries.
 */
public abstract class TimestampedEntry extends RaftLogEntry {
    protected final long timestamp;

    public TimestampedEntry(long term, long timestamp) {
        super(term);
        this.timestamp = timestamp;
    }

    /**
     * Returns the entry timestamp.
     * @return The entry timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .toString();
    }
}

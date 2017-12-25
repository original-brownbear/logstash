package org.logstash.cluster.protocols.raft.storage.log.entry;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.utils.ArraySizeHashPrinter;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * Stores a client keep-alive request.
 */
public class KeepAliveEntry extends TimestampedEntry {
    private final long[] sessionIds;
    private final long[] commandSequences;
    private final long[] eventIndexes;

    public KeepAliveEntry(long term, long timestamp, long[] sessionIds, long[] commandSequences, long[] eventIndexes) {
        super(term, timestamp);
        this.sessionIds = sessionIds;
        this.commandSequences = commandSequences;
        this.eventIndexes = eventIndexes;
    }

    /**
     * Returns the session identifiers.
     * @return The session identifiers.
     */
    public long[] sessionIds() {
        return sessionIds;
    }

    /**
     * Returns the command sequence numbers.
     * @return The command sequence numbers.
     */
    public long[] commandSequenceNumbers() {
        return commandSequences;
    }

    /**
     * Returns the event indexes.
     * @return The event indexes.
     */
    public long[] eventIndexes() {
        return eventIndexes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("term", term)
            .add("timestamp", new TimestampPrinter(timestamp))
            .add("sessionIds", ArraySizeHashPrinter.of(sessionIds))
            .add("commandSequences", ArraySizeHashPrinter.of(commandSequences))
            .add("eventIndexes", ArraySizeHashPrinter.of(eventIndexes))
            .toString();
    }
}

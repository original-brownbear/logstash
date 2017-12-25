package org.logstash.cluster.protocols.raft.storage.log.entry;

/**
 * Metadata entry.
 */
public class MetadataEntry extends SessionEntry {
    public MetadataEntry(long term, long timestamp, long session) {
        super(term, timestamp, session);
    }
}

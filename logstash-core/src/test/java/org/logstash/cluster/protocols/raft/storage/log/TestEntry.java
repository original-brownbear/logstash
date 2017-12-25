package org.logstash.cluster.protocols.raft.storage.log;

import org.logstash.cluster.protocols.raft.storage.log.entry.RaftLogEntry;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Test entry.
 */
public class TestEntry extends RaftLogEntry {
    private final byte[] bytes;

    public TestEntry(long term, int size) {
        this(term, new byte[size]);
    }

    public TestEntry(long term, byte[] bytes) {
        super(term);
        this.bytes = bytes;
    }

    public byte[] bytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("term", term)
            .add("bytes", ArraySizeHashPrinter.of(bytes))
            .toString();
    }
}

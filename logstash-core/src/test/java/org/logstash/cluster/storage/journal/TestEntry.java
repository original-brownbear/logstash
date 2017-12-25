package org.logstash.cluster.storage.journal;

import org.logstash.cluster.utils.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Test entry.
 */
public class TestEntry {
    private final byte[] bytes;

    public TestEntry(int size) {
        this(new byte[size]);
    }

    public TestEntry(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("bytes", ArraySizeHashPrinter.of(bytes))
            .toString();
    }
}

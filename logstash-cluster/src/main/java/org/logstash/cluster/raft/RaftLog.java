package org.logstash.cluster.raft;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public final class RaftLog implements Closeable {

    private final List<RaftLogEntry> entries = new ArrayList<>();

    public RaftLog() {
    }

    public RaftLogEntry get(final long index) {
        if ((long) entries.size() < index + 1L) {
            throw new IllegalStateException(
                String.format("Don't have an entry at index %d.", index)
            );
        }
        return entries.get((int) index);
    }

    public void append(final RaftLogEntry entry) {
        entries.add(entry);
    }

    @Override
    public void close() {
    }
}

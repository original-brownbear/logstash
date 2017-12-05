package org.logstash.cluster.raft;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

public final class RaftLog implements Closeable {

    private final List<RaftLogEntry> entries;

    private final DB database;

    public RaftLog(final DB database) {
        this.database = database;
        this.entries = database.indexTreeList("raftLog", new Serializer<RaftLogEntry>() {
            @Override
            public void serialize(@NotNull final DataOutput2 out, @NotNull final RaftLogEntry value)
                throws IOException {

            }

            @Override
            public RaftLogEntry deserialize(@NotNull final DataInput2 input, final int available)
                throws IOException {
                return null;
            }
        }).createOrOpen();
    }

    public RaftLogEntry get(final long index) {
        if (index >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("Index %d out of bounds", index));
        }
        return entries.get((int) index);
    }

    public void append(final RaftLogEntry entry) {
        entries.add(entry);
    }

    @Override
    public void close() {
        database.commit();
        database.close();
    }
}

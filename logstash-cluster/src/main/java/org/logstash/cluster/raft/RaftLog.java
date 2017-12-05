package org.logstash.cluster.raft;

import java.io.Closeable;
import org.mapdb.DB;

public final class RaftLog implements Closeable {

    private final DB database;

    public RaftLog(final DB database) {
        this.database = database;
    }

    @Override
    public void close() {
        database.close();
    }
}

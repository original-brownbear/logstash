package org.logstash.cluster;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

public final class ClusterStateManager {

    private final ClusterLogState state;

    public ClusterStateManager(final Path stateDir) {
        state = LocalClusterLogState.fromDir(stateDir);
    }

    public Collection<InetSocketAddress> peers() {
        return Collections.emptyList();
    }

    public void apply(final LogEntry entry) {
        this.state.apply(entry);
    }

    public long term() {
        try {
            return state.get("term".getBytes(StandardCharsets.UTF_8)).readLong();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public String votedFor() {
        try {
            return state.get("votedFor".getBytes(StandardCharsets.UTF_8)).readUTF();
        } catch (final IOException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

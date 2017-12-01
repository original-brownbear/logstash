package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;

public final class ClusterClientService implements Runnable, Closeable {

    private final ClusterStateManager state;

    public ClusterClientService(final ClusterStateManager state) {
        this.state = state;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void run() {

    }
}

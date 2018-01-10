package org.logstash.cluster.elasticsearch;

import java.io.Closeable;
import java.io.IOException;
import org.logstash.cluster.EnqueueEvent;

public final class EsQueue implements Closeable {

    public void pushTask(final EnqueueEvent task) {

    }

    public void complete(final EnqueueEvent task) {

    }

    public EnqueueEvent nextTask() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}

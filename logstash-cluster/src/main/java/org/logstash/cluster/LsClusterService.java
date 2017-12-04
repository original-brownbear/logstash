package org.logstash.cluster;

import java.io.Closeable;

public interface LsClusterService extends Runnable, Closeable {

    void stop();

    void awaitStop();
}

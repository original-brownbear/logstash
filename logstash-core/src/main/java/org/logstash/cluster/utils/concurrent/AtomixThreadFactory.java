package org.logstash.cluster.utils.concurrent;

import java.util.concurrent.ThreadFactory;

/**
 * Named thread factory.
 */
public class AtomixThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(Runnable r) {
        return new AtomixThread(r);
    }
}

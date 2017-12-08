package org.logstash.cluster.utils.concurrent;

import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Single thread context factory.
 */
public class SingleThreadContextFactory implements ThreadContextFactory {
    private final ThreadFactory threadFactory;

    public SingleThreadContextFactory(String nameFormat, Logger logger) {
        this(Threads.namedThreads(nameFormat, logger));
    }

    public SingleThreadContextFactory(ThreadFactory threadFactory) {
        this.threadFactory = checkNotNull(threadFactory);
    }

    @Override
    public ThreadContext createContext() {
        return new SingleThreadContext(threadFactory);
    }
}

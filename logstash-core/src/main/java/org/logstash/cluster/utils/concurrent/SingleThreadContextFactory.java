package org.logstash.cluster.utils.concurrent;

import com.google.common.base.Preconditions;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.Logger;

/**
 * Single thread context factory.
 */
public class SingleThreadContextFactory implements ThreadContextFactory {
    private final ThreadFactory threadFactory;

    public SingleThreadContextFactory(String nameFormat, Logger logger) {
        this(Threads.namedThreads(nameFormat, logger));
    }

    public SingleThreadContextFactory(ThreadFactory threadFactory) {
        this.threadFactory = Preconditions.checkNotNull(threadFactory);
    }

    @Override
    public ThreadContext createContext() {
        return new SingleThreadContext(threadFactory);
    }
}

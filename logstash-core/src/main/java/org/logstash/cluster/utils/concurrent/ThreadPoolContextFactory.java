package org.logstash.cluster.utils.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.Logger;

/**
 * Thread pool context factory.
 */
public class ThreadPoolContextFactory implements ThreadContextFactory {
    private final ScheduledExecutorService executor;

    public ThreadPoolContextFactory(String name, int threadPoolSize, Logger logger) {
        this(threadPoolSize, Threads.namedThreads(name, logger));
    }

    public ThreadPoolContextFactory(int threadPoolSize, ThreadFactory threadFactory) {
        this(Executors.newScheduledThreadPool(threadPoolSize, threadFactory));
    }

    public ThreadPoolContextFactory(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public ThreadContext createContext() {
        return new ThreadPoolContext(executor);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}

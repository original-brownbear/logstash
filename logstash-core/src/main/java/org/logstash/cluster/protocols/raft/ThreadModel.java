package org.logstash.cluster.protocols.raft;

import org.apache.logging.log4j.Logger;
import org.logstash.cluster.utils.concurrent.SingleThreadContextFactory;
import org.logstash.cluster.utils.concurrent.ThreadContextFactory;
import org.logstash.cluster.utils.concurrent.ThreadPoolContextFactory;

/**
 * Raft thread model.
 */
public enum ThreadModel {

    /**
     * A thread model that creates a thread pool to be shared by all services.
     */
    SHARED_THREAD_POOL {
        @Override
        public ThreadContextFactory factory(String nameFormat, int threadPoolSize, Logger logger) {
            return new ThreadPoolContextFactory(nameFormat, threadPoolSize, logger);
        }
    },

    /**
     * A thread model that creates a thread for each Raft service.
     */
    THREAD_PER_SERVICE {
        @Override
        public ThreadContextFactory factory(String nameFormat, int threadPoolSize, Logger logger) {
            return new SingleThreadContextFactory(nameFormat, logger);
        }
    };

    /**
     * Returns a thread context factory.
     * @param nameFormat the thread name format
     * @param threadPoolSize the thread pool size
     * @param logger the thread logger
     * @return the thread context factory
     */
    public abstract ThreadContextFactory factory(String nameFormat, int threadPoolSize, Logger logger);
}

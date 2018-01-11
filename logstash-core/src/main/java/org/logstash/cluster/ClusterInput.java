package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.elasticsearch.primitives.EsLock;
import org.logstash.cluster.elasticsearch.primitives.EsQueue;
import org.logstash.cluster.execution.HeartbeatAction;
import org.logstash.cluster.execution.LeaderElectionAction;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JavaQueue;

public final class ClusterInput implements Runnable, Closeable {

    public static final String LOGSTASH_TASK_CLASS_SETTING = "lstaskclass";

    public static final String LEADERSHIP_IDENTIFIER = "lsclusterleader";

    public static final String TASK_QUEUE_NAME = "logstashWorkQueue";

    private static final Logger LOGGER = LogManager.getLogger(ClusterInput.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final CountDownLatch done = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final EsClient esClient;

    private final EventQueue queue;

    private final EsQueue tasks;

    private final EsLock leaderLock;

    public ClusterInput(final IRubyObject queue, final EsClient provider) {
        this(new JavaQueue(queue), provider);
    }

    public ClusterInput(final EventQueue queue, final EsClient provider) {
        this.queue = queue;
        this.esClient = provider;
        tasks = esClient.queue(TASK_QUEUE_NAME);
        leaderLock = esClient.lock(ClusterInput.LEADERSHIP_IDENTIFIER);
    }

    public Map<String, Object> getConfig() {
        return esClient.currentJobSettings();
    }

    public EsClient getEsClient() {
        return esClient;
    }

    public EsQueue getTasks() {
        return tasks;
    }

    public EsLock leaderLock() {
        return leaderLock;
    }

    public ScheduledExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void run() {
        executor.scheduleAtFixedRate(
            new HeartbeatAction(esClient), 0L, 5L, TimeUnit.SECONDS
        );
        executor.scheduleAtFixedRate(
            new LeaderElectionAction(this),
            0L, LeaderElectionAction.ELECTION_PERIOD,
            TimeUnit.MILLISECONDS
        );
        try {
            while (running.get()) {
                final WorkerTask task = tasks.nextTask();
                if (task != null) {
                    task.enqueueEvents(this, queue);
                    tasks.complete(task);
                }
            }
        } finally {
            leaderLock.unlock();
            done.countDown();
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing cluster input.");
        if (running.compareAndSet(true, false)) {
            tasks.close();
            try {
                done.await();
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(ex);
            } finally {
                executor.shutdownNow();
                try {
                    if (!executor.awaitTermination(2L, TimeUnit.MINUTES)) {
                        throw new IllegalStateException("Failed to stop task service");
                    }
                } catch (final InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
                LOGGER.info("Closed cluster input.");
            }
        }
    }
}

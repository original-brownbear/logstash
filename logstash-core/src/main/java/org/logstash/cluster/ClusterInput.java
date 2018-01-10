package org.logstash.cluster;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.cluster.elasticsearch.EsClient;
import org.logstash.cluster.elasticsearch.EsQueue;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JavaQueue;

public final class ClusterInput implements Runnable, Closeable {

    public static final String LOGSTASH_TASK_CLASS_SETTING = "lstaskclass";

    public static final String LEADERSHIP_IDENTIFIER = "lsclusterleader";

    public static final String TASK_QUEUE_NAME = "logstashWorkQueue";

    private static final Logger LOGGER = LogManager.getLogger(ClusterInput.class);

    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    private final CountDownLatch done = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final EsClient esClient;

    private final EventQueue queue;

    private final EsQueue tasks;

    private ClusterInput.LeaderTask leaderTask;

    public ClusterInput(final IRubyObject queue, final EsClient provider) {
        this(new JavaQueue(queue), provider);
    }

    public ClusterInput(final EventQueue queue, final EsClient provider) {
        this.queue = queue;
        this.esClient = provider;
        tasks = provider.queue(TASK_QUEUE_NAME);
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

    @Override
    public void run() {
        executor.submit(new ClusterInput.BackgroundLoop(esClient));
        try {
            synchronized (this) {
                leaderTask = setupLeaderTask();
                if (leaderTask != null) {
                    executor.submit(leaderTask);
                } else {
                    return;
                }
            }
            while (running.get()) {
                final EnqueueEvent task = tasks.nextTask();
                if (task != null) {
                    task.enqueue(this, queue);
                    tasks.complete(task);
                }
            }
        } finally {
            done.countDown();
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("Closing cluster input.");
        if (running.compareAndSet(true, false)) {
            synchronized (this) {
                if (leaderTask != null) {
                    leaderTask.stop();
                    leaderTask.awaitStop();
                }
            }
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

    private ClusterInput.LeaderTask setupLeaderTask() {
        try {
            Map<String, Object> configuration;
            while (!(configuration = getConfig()).containsKey(LOGSTASH_TASK_CLASS_SETTING)) {
                LOGGER.info(
                    "No valid cluster input configuration found, sleeping 5s before retrying."
                );
                TimeUnit.SECONDS.sleep(5L);
                if (!running.get()) {
                    LOGGER.warn("Gave up trying to configure cluster leader task");
                    return null;
                }
            }
            return Class.forName((String) configuration.get(LOGSTASH_TASK_CLASS_SETTING))
                .asSubclass(ClusterInput.LeaderTask.class).getConstructor(ClusterInput.class)
                .newInstance(this);
        } catch (final Exception ex) {
            LOGGER.error("Failed to set up leader task because of: {}", ex);
            throw new IllegalStateException(ex);
        }
    }

    public interface LeaderTask extends Runnable {

        void awaitStop();

        void stop();
    }

    private static final class BackgroundLoop implements Runnable {

        private static final Logger LOGGER = LogManager.getLogger(ClusterInput.BackgroundLoop.class);

        private final EsClient client;

        BackgroundLoop(final EsClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            while (true) {
                final String local = client.getConfig().localNode();
                if (!client.currentClusterNodes().contains(local)) {
                    LOGGER.info("Publishing local node {} since it wasn't found in the node list.", local);
                    client.publishLocalNode();
                }
            }
        }
    }
}

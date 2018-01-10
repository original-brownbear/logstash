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
import org.logstash.cluster.elasticsearch.EsLock;
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
        executor.submit(new ClusterInput.BackgroundHeartbeatLoop(esClient));
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
            final String clazz = (String) configuration.get(LOGSTASH_TASK_CLASS_SETTING);
            LOGGER.info(
                "Found valid cluster input configuration, starting leader task of type {}.",
                clazz
            );
            return Class.forName(clazz)
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

    private static final class BackgroundLeaderElectionLoop implements Runnable {

        private static final Logger LOGGER =
            LogManager.getLogger(ClusterInput.BackgroundLeaderElectionLoop.class);

        private final EsClient client;

        BackgroundLeaderElectionLoop(final EsClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            final String local = client.getConfig().localNode();
            LOGGER.info("Started background leader election loop on {}", local);
            while (!Thread.currentThread().isInterrupted()) {
                final EsLock leaderLock = client.lock(LEADERSHIP_IDENTIFIER);
                final long expire = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30L);
                LOGGER.info("Trying to acquire leader lock until {} on {}", expire, local);
                if (leaderLock.lock(expire)) {

                } else {

                }
            }
        }
    }

    private static final class BackgroundHeartbeatLoop implements Runnable {

        private static final Logger LOGGER =
            LogManager.getLogger(ClusterInput.BackgroundHeartbeatLoop.class);

        private final EsClient client;

        BackgroundHeartbeatLoop(final EsClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            final String local = client.getConfig().localNode();
            LOGGER.info("Started background heartbeat loop on {}", local);
            while (!Thread.currentThread().isInterrupted()) {
                if (!client.currentClusterNodes().contains(local)) {
                    LOGGER.info(
                        "Publishing local node {} since it wasn't found in the node list.",
                        local
                    );
                    client.publishLocalNode();
                    LOGGER.info("Published local node {} to node list.", local);
                }
                try {
                    TimeUnit.SECONDS.sleep(5L);
                } catch (final InterruptedException ex) {
                    LOGGER.error("Background heartbeat loop on {} interrupted", local);
                    throw new IllegalStateException(ex);
                }
            }
        }
    }
}

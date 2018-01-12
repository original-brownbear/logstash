package org.logstash.cluster;

import java.io.Closeable;
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
import org.logstash.cluster.state.TaskQueue;
import org.logstash.cluster.execution.LeaderElectionAction;
import org.logstash.cluster.execution.TimingConstants;
import org.logstash.cluster.execution.WorkerHeartbeatAction;
import org.logstash.cluster.state.Task;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JavaQueue;

public final class ClusterInput implements Runnable, Closeable {

    public static final String LOGSTASH_TASK_CLASS_SETTING = "lstaskclass";

    public static final String LEADERSHIP_IDENTIFIER = "lsclusterleader";

    private static final Logger LOGGER = LogManager.getLogger(ClusterInput.class);

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private final CountDownLatch done = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final EsClient esClient;

    private final EventQueue queue;

    private final TaskQueue tasks;

    private final EsLock leaderLock;

    public ClusterInput(final IRubyObject queue, final EsClient esClient) {
        this(new JavaQueue(queue), esClient);
    }

    public ClusterInput(final EventQueue queue, final EsClient esClient) {
        this.queue = queue;
        this.esClient = esClient;
        tasks = this.esClient.taskQueue();
        leaderLock = this.esClient.lock(ClusterInput.LEADERSHIP_IDENTIFIER);
    }

    public Map<String, Object> getConfig() {
        return esClient.currentJobSettings();
    }

    public EsClient getEsClient() {
        return esClient;
    }

    public TaskQueue getTasks() {
        return tasks;
    }

    @Override
    public void run() {
        final String localNode = esClient.getConfig().localNode();
        executor.scheduleAtFixedRate(
            new WorkerHeartbeatAction(esClient), 0L,
            TimingConstants.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS
        );
        LOGGER.info("Started background heartbeat loop on {}", localNode);
        try (LeaderElectionAction leaderAction = new LeaderElectionAction(this)) {
            executor.scheduleAtFixedRate(
                leaderAction,
                0L, TimingConstants.ELECTION_PERIOD,
                TimeUnit.MILLISECONDS
            );
            while (running.get()) {
                final Task task = tasks.nextTask();
                if (task != null) {
                    LOGGER.info("Running new task number {}", task.getId(), localNode);
                    task.getTask().enqueueEvents(this, queue);
                    task.complete();
                    LOGGER.info("Completed task {} on {}", task.getId(), localNode);
                }
            }
        } catch (final Exception ex) {
            LOGGER.error("Cluster input main loop died because of:", ex);
            throw new IllegalStateException(ex);
        } finally {
            leaderLock.unlock();
            done.countDown();
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing cluster input.");
        if (running.compareAndSet(true, false)) {
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

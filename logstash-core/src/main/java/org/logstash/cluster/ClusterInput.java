package org.logstash.cluster;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JavaQueue;

public final class ClusterInput implements Runnable, Closeable {

    public static final String LOGSTASH_TASK_CLASS_SETTING = "lstaskclass";

    public static final String LEADERSHIP_IDENTIFIER = "lsclusterleader";

    public static final String P2P_QUEUE_NAME = "logstashWorkQueue";

    private static final Logger LOGGER = LogManager.getLogger(ClusterInput.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final CountDownLatch done = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final LogstashClusterServer cluster;

    private final ClusterConfigProvider configProvider;

    private final EventQueue queue;

    private final AsyncWorkQueue<EnqueueEvent> tasks;

    private final LeaderTask leaderTask;

    public ClusterInput(final IRubyObject queue, final ClusterConfigProvider provider) {
        this(new JavaQueue(queue), provider);
    }

    public ClusterInput(final EventQueue queue, final ClusterConfigProvider provider) {
        this.queue = queue;
        this.configProvider = provider;
        cluster = LogstashClusterServer.fromConfig(provider.currentClusterConfig());
        tasks = cluster.<EnqueueEvent>workQueueBuilder().withName(P2P_QUEUE_NAME)
            .withSerializer(Serializer.JAVA).buildAsync();
        leaderTask = setupLeaderTask();
    }

    public Map<String, String> getConfig() {
        return configProvider.currentJobSettings();
    }

    public LogstashClusterServer getCluster() {
        return cluster;
    }

    public AsyncWorkQueue<EnqueueEvent> getTasks() {
        return tasks;
    }

    @Override
    public void run() {
        executor.submit(leaderTask);
        try {
            final WorkQueue<EnqueueEvent> work = tasks.asWorkQueue();
            while (running.get()) {
                final Task<EnqueueEvent> task = work.take();
                if (task != null) {
                    task.payload().enqueue(queue);
                    work.complete(task.taskId());
                }
            }
        } finally {
            done.countDown();
        }
    }

    @Override
    public void close() {
        LOGGER.info("Closing cluster input.");
        running.set(false);
        leaderTask.stop();
        leaderTask.awaitStop();
        tasks.close().join();
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
            cluster.close().join();
            LOGGER.info("Closed cluster input.");
        }
    }

    private ClusterInput.LeaderTask setupLeaderTask() {
        try {
            return Class.forName(getConfig().get(LOGSTASH_TASK_CLASS_SETTING))
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
}

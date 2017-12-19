package org.logstash.cluster;

import java.io.Closeable;
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

    public static final String P2P_QUEUE_NAME = "logstashWorkQueue";

    private static final Logger LOGGER = LogManager.getLogger(ClusterInput.class);

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final CountDownLatch done = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final LogstashClusterServer cluster;

    private final EventQueue queue;

    private final AsyncWorkQueue<EnqueueEvent> tasks;

    public ClusterInput(final IRubyObject queue, final ClusterConfigProvider provider) {
        this(new JavaQueue(queue), provider);
    }

    ClusterInput(final EventQueue queue, final ClusterConfigProvider provider) {
        this.queue = queue;
        cluster = LogstashClusterServer.fromConfig(provider.currentConfig());
        tasks = cluster.<EnqueueEvent>workQueueBuilder().withName(P2P_QUEUE_NAME)
            .withSerializer(Serializer.JAVA).buildAsync();
    }

    @Override
    public void run() {
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
        tasks.close().join();
        try {
            done.await();
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        } finally {
            cluster.close().join();
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

package org.logstash.plugins.input;

import java.io.Closeable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jruby.runtime.builtin.IRubyObject;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.cluster.LogstashClusterServer;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.ext.EventQueue;
import org.logstash.ext.JavaQueue;

public final class ClusterInput implements Runnable, Closeable {

    public static final String P2P_QUEUE_NAME = "logstashWorkQueue";

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final CountDownLatch stopped = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final LogstashClusterServer cluster;

    private final EventQueue queue;

    private final WorkQueue<EnqueueEvent> tasks;

    public ClusterInput(final IRubyObject queue, final LogstashClusterConfig config) {
        this(new JavaQueue(queue), config);
    }

    public ClusterInput(final EventQueue queue, final LogstashClusterConfig config) {
        this.queue = queue;
        cluster = LogstashClusterServer.builder().withLocalNode(config.localNode())
            .withBootstrapNodes(config.getBootstrap())
            .withDataDir(config.getDataDir())
            .withNumPartitions(1)
            .build()
            .open().join();
        tasks = cluster.<EnqueueEvent>workQueueBuilder().withName(P2P_QUEUE_NAME)
            .withSerializer(Serializer.JAVA).build();
    }

    @Override
    public void run() {
        while (running.get()) {
            final Task<EnqueueEvent> task = tasks.take();
            if (task != null) {
                task.payload().enqueue(queue);
                tasks.complete(task.taskId());
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        tasks.close();
        cluster.close().thenRun(stopped::countDown);
        awaitStop();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2L, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Failed to stop task service");
            }
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void awaitStop() {
        try {
            stopped.await();
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

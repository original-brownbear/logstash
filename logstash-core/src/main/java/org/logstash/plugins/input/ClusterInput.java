package org.logstash.plugins.input;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.logstash.cluster.LogstashCluster;
import org.logstash.cluster.LogstashClusterConfig;
import org.logstash.ext.JavaQueue;

public final class ClusterInput implements Runnable, Closeable {

    private final BlockingQueue<EnqueueEvent> tasks = new LinkedTransferQueue<>();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final CountDownLatch stopped = new CountDownLatch(1);

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final JavaQueue queue;

    private final ClusterInput.TaskService taskService;

    public ClusterInput(final JavaQueue queue, final LogstashClusterConfig config) {
        this.queue = queue;
        this.taskService = new ClusterInput.TaskService(tasks, config);
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                final EnqueueEvent task = tasks.poll(200L, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.enqueue(queue);
                }
            }
        } catch (final InterruptedException ex) {
            running.set(false);
            throw new IllegalStateException(ex);
        } finally {
            stopped.countDown();
        }
    }

    public void stop() {
        running.set(false);
    }

    public void awaitStop() {
        try {
            stopped.await();
        } catch (final InterruptedException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
        taskService.close();
        stop();
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

    private static final class TaskService implements Runnable, Closeable {

        private final BlockingQueue<EnqueueEvent> tasks;

        private final LogstashCluster cluster;

        TaskService(final BlockingQueue<EnqueueEvent> tasks, final LogstashClusterConfig config) {
            this.tasks = tasks;
            cluster = LogstashCluster.builder().withLocalNode(config.localNode())
                .withBootstrapNodes(config.getBootstrap()).withDataDir(config.getDataDir()).build();
        }

        @Override
        public void run() {
            cluster.open().join();
            while (cluster.isOpen()) {
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (final InterruptedException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }

        @Override
        public void close() {
            cluster.close().join();
        }
    }
}

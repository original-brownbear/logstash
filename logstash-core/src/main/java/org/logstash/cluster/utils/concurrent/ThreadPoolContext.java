package org.logstash.cluster.utils.concurrent;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread pool context.
 * <p>
 * This is a special {@link ThreadContext} implementation that schedules events to be executed
 * on a thread pool. Events executed by this context are guaranteed to be executed on order but may be executed on different
 * threads in the provided thread pool.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ThreadPoolContext implements ThreadContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadPoolContext.class);
    private final ScheduledExecutorService parent;
    private final Runnable runner;
    private final LinkedList<Runnable> tasks = new LinkedList<>();
    private boolean running;
    private final Executor executor = new Executor() {
        @Override
        public void execute(Runnable command) {
            synchronized (tasks) {
                tasks.add(command);
                if (!running) {
                    running = true;
                    parent.execute(runner);
                }
            }
        }
    };

    /**
     * Creates a new thread pool context.
     * @param parent The thread pool on which to execute events.
     */
    public ThreadPoolContext(ScheduledExecutorService parent) {
        this.parent = Preconditions.checkNotNull(parent, "parent cannot be null");

        // This code was shamelessly stolededed from Vert.x:
        // https://github.com/eclipse/vert.x/blob/master/src/main/java/io/vertx/core/impl/OrderedExecutorFactory.java
        runner = () -> {
            ((AtomixThread) Thread.currentThread()).setContext(this);
            for (; ; ) {
                final Runnable task;
                synchronized (tasks) {
                    task = tasks.poll();
                    if (task == null) {
                        running = false;
                        return;
                    }
                }

                try {
                    task.run();
                } catch (Throwable t) {
                    LOGGER.error("An uncaught exception occurred", t);
                    throw t;
                }
            }
        };
    }

    @Override
    public Scheduled schedule(Duration delay, Runnable runnable) {
        ScheduledFuture<?> future = parent.schedule(() -> executor.execute(runnable), delay.toMillis(), TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }

    @Override
    public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
        ScheduledFuture<?> future = parent.scheduleAtFixedRate(() -> executor.execute(runnable), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public void close() {
        // Do nothing.
    }

}

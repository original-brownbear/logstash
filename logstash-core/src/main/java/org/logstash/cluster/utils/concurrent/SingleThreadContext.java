package org.logstash.cluster.utils.concurrent;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Single threaded context.
 * <p>
 * This is a basic {@link ThreadContext} implementation that uses a
 * {@link ScheduledExecutorService} to schedule events on the context thread.
 */
public class SingleThreadContext implements ThreadContext {
    protected static final Logger LOGGER = LogManager.getLogger(SingleThreadContext.class);
    private final ScheduledExecutorService executor;
    private final Executor wrappedExecutor = new Executor() {
        @Override
        public void execute(final Runnable command) {
            try {
                executor.execute(command);
            } catch (final RejectedExecutionException e) {
            }
        }
    };

    /**
     * Creates a new single thread context.
     * <p>
     * The provided context name will be passed to {@link AtomixThreadFactory} and used
     * when instantiating the context thread.
     * @param nameFormat The context nameFormat which will be formatted with a thread number.
     */
    public SingleThreadContext(final String nameFormat) {
        this(Threads.namedThreads(nameFormat, LOGGER));
    }

    /**
     * Creates a new single thread context.
     * @param factory The thread factory.
     */
    public SingleThreadContext(final ThreadFactory factory) {
        this(new ScheduledThreadPoolExecutor(1, factory));
    }

    /**
     * Creates a new single thread context.
     * @param executor The executor on which to schedule events. This must be a single thread scheduled executor.
     */
    private SingleThreadContext(final ScheduledExecutorService executor) {
        this(getThread(executor), executor);
    }

    private SingleThreadContext(final Thread thread, final ScheduledExecutorService executor) {
        this.executor = executor;
        Preconditions.checkState(thread instanceof AtomixThread, "not a Catalyst thread");
        ((AtomixThread) thread).setContext(this);
    }

    /**
     * Gets the thread from a single threaded executor service.
     */
    protected static AtomixThread getThread(final ExecutorService executor) {
        final AtomicReference<AtomixThread> thread = new AtomicReference<>();
        try {
            executor.submit(() -> thread.set((AtomixThread) Thread.currentThread())).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("failed to initialize thread state", e);
        }
        return thread.get();
    }

    @Override
    public Scheduled schedule(final Duration delay, final Runnable runnable) {
        final ScheduledFuture<?> future = executor.schedule(runnable, delay.toMillis(), TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public void execute(final Runnable command) {
        wrappedExecutor.execute(command);
    }

    @Override
    public Scheduled schedule(final Duration delay, final Duration interval, final Runnable runnable) {
        final ScheduledFuture<?> future = executor.scheduleAtFixedRate(runnable, delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        return () -> future.cancel(false);
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

}

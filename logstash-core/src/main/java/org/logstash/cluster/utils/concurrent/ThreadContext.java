package org.logstash.cluster.utils.concurrent;

import com.google.common.base.Preconditions;
import java.util.concurrent.Executor;

/**
 * Thread context.
 * <p>
 * The thread context is used by Catalyst to determine the correct thread on which to execute asynchronous callbacks.
 * All threads created within Catalyst must be instances of {@link AtomixThread}. Once
 * a thread has been created, the context is stored in the thread object via
 * {@link AtomixThread#setContext(ThreadContext)}. This means there is a one-to-one relationship
 * between a context and a thread. That is, a context is representative of a thread and provides an interface for firing
 * events on that thread.
 * <p>
 * Components of the framework that provide custom threads should use {@link AtomixThreadFactory}
 * to allocate new threads and provide a custom {@link ThreadContext} implementation.
 */
public interface ThreadContext extends AutoCloseable, Executor, Scheduler {

    /**
     * Returns the current thread context.
     * @return The current thread context or {@code null} if no context exists.
     */
    static ThreadContext currentContext() {
        Thread thread = Thread.currentThread();
        return thread instanceof AtomixThread ? ((AtomixThread) thread).getContext() : null;
    }

    /**
     * Returns a boolean indicating whether the current thread is in this context.
     * @return Indicates whether the current thread is in this context.
     */
    default boolean isCurrentContext() {
        return currentContext() == this;
    }

    /**
     * Checks that the current thread is the correct context thread.
     */
    default void checkThread() {
        Preconditions.checkState(currentContext() == this, "not on a Catalyst thread");
    }

    /**
     * Closes the context.
     */
    @Override
    void close();

}

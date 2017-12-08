package org.logstash.cluster.utils.concurrent;

import java.lang.ref.WeakReference;

/**
 * Atomix thread.
 * <p>
 * The Atomix thread primarily serves to store a {@link ThreadContext} for the current thread.
 * The context is stored in a {@link WeakReference} in order to allow the thread to be garbage collected.
 * <p>
 * There is no {@link ThreadContext} associated with the thread when it is first created.
 * It is the responsibility of thread creators to {@link #setContext(ThreadContext) set} the thread context when appropriate.
 */
public class AtomixThread extends Thread {
    private WeakReference<ThreadContext> context;

    public AtomixThread(Runnable target) {
        super(target);
    }

    /**
     * Returns the thread context.
     * @return The thread {@link ThreadContext} or {@code null} if no context has been configured.
     */
    public ThreadContext getContext() {
        return context != null ? context.get() : null;
    }

    /**
     * Sets the thread context.
     * @param context The thread context.
     */
    public void setContext(ThreadContext context) {
        this.context = new WeakReference<>(context);
    }

}
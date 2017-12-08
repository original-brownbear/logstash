package org.logstash.cluster.primitives.counter.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.counter.AsyncAtomicCounter;
import org.logstash.cluster.primitives.counter.AtomicCounter;

/**
 * Default implementation for a {@code AtomicCounter} backed by a {@link AsyncAtomicCounter}.
 */
public class BlockingAtomicCounter extends Synchronous<AsyncAtomicCounter> implements AtomicCounter {

    private final AsyncAtomicCounter asyncCounter;
    private final long operationTimeoutMillis;

    public BlockingAtomicCounter(AsyncAtomicCounter asyncCounter, long operationTimeoutMillis) {
        super(asyncCounter);
        this.asyncCounter = asyncCounter;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public long incrementAndGet() {
        return complete(asyncCounter.incrementAndGet());
    }

    @Override
    public long getAndIncrement() {
        return complete(asyncCounter.getAndIncrement());
    }

    @Override
    public long getAndAdd(long delta) {
        return complete(asyncCounter.getAndAdd(delta));
    }

    @Override
    public long addAndGet(long delta) {
        return complete(asyncCounter.addAndGet(delta));
    }

    @Override
    public void set(long value) {
        complete(asyncCounter.set(value));
    }

    @Override
    public boolean compareAndSet(long expectedValue, long updateValue) {
        return complete(asyncCounter.compareAndSet(expectedValue, updateValue));
    }

    @Override
    public long get() {
        return complete(asyncCounter.get());
    }

    private <T> T complete(CompletableFuture<T> future) {
        try {
            return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PrimitiveException.Interrupted();
        } catch (TimeoutException e) {
            throw new PrimitiveException.Timeout();
        } catch (ExecutionException e) {
            throw new PrimitiveException(e.getCause());
        }
    }
}
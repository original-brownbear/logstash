package org.logstash.cluster.primitives.value.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.primitives.value.AtomicValue;
import org.logstash.cluster.primitives.value.AtomicValueEventListener;

/**
 * Default implementation for a {@code AtomicValue} backed by a {@link AsyncAtomicValue}.
 * @param <V> value type
 */
public class BlockingAtomicValue<V> extends Synchronous<AsyncAtomicValue<V>> implements AtomicValue<V> {

    private final AsyncAtomicValue<V> asyncValue;
    private final long operationTimeoutMillis;

    public BlockingAtomicValue(AsyncAtomicValue<V> asyncValue, long operationTimeoutMillis) {
        super(asyncValue);
        this.asyncValue = asyncValue;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public boolean compareAndSet(V expect, V update) {
        return complete(asyncValue.compareAndSet(expect, update));
    }

    @Override
    public V get() {
        return complete(asyncValue.get());
    }

    @Override
    public V getAndSet(V value) {
        return complete(asyncValue.getAndSet(value));
    }

    @Override
    public void set(V value) {
        complete(asyncValue.set(value));
    }

    @Override
    public void addListener(AtomicValueEventListener<V> listener) {
        complete(asyncValue.addListener(listener));
    }

    @Override
    public void removeListener(AtomicValueEventListener<V> listener) {
        complete(asyncValue.removeListener(listener));
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

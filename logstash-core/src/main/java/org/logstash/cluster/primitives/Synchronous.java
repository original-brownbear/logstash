package org.logstash.cluster.primitives;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DistributedPrimitive that is a synchronous (blocking) version of
 * another.
 * @param <T> type of DistributedPrimitive
 */
public abstract class Synchronous<T extends AsyncPrimitive> implements SyncPrimitive {

    private final T primitive;

    public Synchronous(T primitive) {
        this.primitive = primitive;
    }

    @Override
    public String name() {
        return primitive.name();
    }

    @Override
    public DistributedPrimitive.Type primitiveType() {
        return primitive.primitiveType();
    }

    @Override
    public void destroy() {
        try {
            primitive.destroy().get(DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
        try {
            primitive.close().get(DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

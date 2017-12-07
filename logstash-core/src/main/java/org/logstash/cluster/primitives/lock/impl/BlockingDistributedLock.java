package org.logstash.cluster.primitives.lock.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.lock.AsyncDistributedLock;
import org.logstash.cluster.primitives.lock.DistributedLock;
import org.logstash.cluster.time.Version;

/**
 * Default implementation for a {@code DistributedLock} backed by a {@link AsyncDistributedLock}.
 */
public class BlockingDistributedLock extends Synchronous<AsyncDistributedLock> implements DistributedLock {

    private final AsyncDistributedLock asyncLock;
    private final long operationTimeoutMillis;

    public BlockingDistributedLock(AsyncDistributedLock asyncLock, long operationTimeoutMillis) {
        super(asyncLock);
        this.asyncLock = asyncLock;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public Version lock() {
        return complete(asyncLock.lock());
    }

    @Override
    public Optional<Version> tryLock() {
        return complete(asyncLock.tryLock());
    }

    @Override
    public Optional<Version> tryLock(Duration timeout) {
        return complete(asyncLock.tryLock(timeout));
    }

    @Override
    public void unlock() {
        complete(asyncLock.unlock());
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

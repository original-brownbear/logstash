package org.logstash.cluster.primitives.generator.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.generator.AsyncAtomicIdGenerator;
import org.logstash.cluster.primitives.generator.AtomicIdGenerator;

/**
 * Default implementation for a {@code AtomicIdGenerator} backed by a {@link AsyncAtomicIdGenerator}.
 */
public class BlockingAtomicIdGenerator extends Synchronous<AsyncAtomicIdGenerator> implements AtomicIdGenerator {

    private final AsyncAtomicIdGenerator asyncIdGenerator;
    private final long operationTimeoutMillis;

    public BlockingAtomicIdGenerator(AsyncAtomicIdGenerator asyncIdGenerator, long operationTimeoutMillis) {
        super(asyncIdGenerator);
        this.asyncIdGenerator = asyncIdGenerator;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public long nextId() {
        return complete(asyncIdGenerator.nextId());
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

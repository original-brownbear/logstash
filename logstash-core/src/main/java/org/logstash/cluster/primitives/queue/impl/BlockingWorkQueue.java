package org.logstash.cluster.primitives.queue.impl;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.logstash.cluster.primitives.PrimitiveException;
import org.logstash.cluster.primitives.Synchronous;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueue;
import org.logstash.cluster.primitives.queue.WorkQueueStats;

/**
 * Default synchronous work queue implementation.
 */
public class BlockingWorkQueue<E> extends Synchronous<AsyncWorkQueue<E>> implements WorkQueue<E> {

    private final AsyncWorkQueue<E> asyncQueue;
    private final long operationTimeoutMillis;

    public BlockingWorkQueue(AsyncWorkQueue<E> asyncQueue, long operationTimeoutMillis) {
        super(asyncQueue);
        this.asyncQueue = asyncQueue;
        this.operationTimeoutMillis = operationTimeoutMillis;
    }

    @Override
    public void registerTaskProcessor(Consumer<E> taskProcessor, int parallelism, Executor executor) {
        complete(asyncQueue.registerTaskProcessor(taskProcessor, parallelism, executor));
    }

    @Override
    public void stopProcessing() {
        complete(asyncQueue.stopProcessing());
    }

    @Override
    public WorkQueueStats stats() {
        return complete(asyncQueue.stats());
    }

    @Override
    public void complete(Collection<String> taskIds) {
        complete(asyncQueue.complete(taskIds));
    }

    @Override
    public void addMultiple(Collection<E> items) {
        complete(asyncQueue.addMultiple(items));
    }

    @Override
    public Collection<Task<E>> take(int maxItems) {
        return complete(asyncQueue.take(maxItems));
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

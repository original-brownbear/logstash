package org.logstash.cluster.primitives.queue.impl;

import com.google.common.base.MoreObjects;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueueStats;

/**
 * Transcoding async work queue.
 */
public class TranscodingAsyncWorkQueue<V1, V2> implements AsyncWorkQueue<V1> {

    private final AsyncWorkQueue<V2> backingQueue;
    private final Function<V1, V2> valueEncoder;
    private final Function<V2, V1> valueDecoder;

    public TranscodingAsyncWorkQueue(AsyncWorkQueue<V2> backingQueue, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
        this.backingQueue = backingQueue;
        this.valueEncoder = valueEncoder;
        this.valueDecoder = valueDecoder;
    }

    @Override
    public String name() {
        return backingQueue.name();
    }

    @Override
    public CompletableFuture<Void> registerTaskProcessor(Consumer<V1> taskProcessor, int parallelism, Executor executor) {
        return backingQueue.registerTaskProcessor(v -> taskProcessor.accept(valueDecoder.apply(v)), parallelism, executor);
    }

    @Override
    public CompletableFuture<Void> stopProcessing() {
        return backingQueue.stopProcessing();
    }

    @Override
    public CompletableFuture<WorkQueueStats> stats() {
        return backingQueue.stats();
    }

    @Override
    public CompletableFuture<Void> complete(Collection<String> taskIds) {
        return backingQueue.complete(taskIds);
    }

    @Override
    public CompletableFuture<Void> addMultiple(Collection<V1> items) {
        return backingQueue.addMultiple(items.stream().map(valueEncoder).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Collection<Task<V1>>> take(int maxItems) {
        return backingQueue.take(maxItems)
            .thenApply(tasks -> tasks.stream().map(t -> t.map(valueDecoder))
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Void> close() {
        return backingQueue.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("backingValue", backingQueue)
            .toString();
    }
}

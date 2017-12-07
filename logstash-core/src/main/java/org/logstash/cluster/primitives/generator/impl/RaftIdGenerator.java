package org.logstash.cluster.primitives.generator.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.logstash.cluster.primitives.counter.AsyncAtomicCounter;
import org.logstash.cluster.primitives.generator.AsyncAtomicIdGenerator;

/**
 * {@code AsyncAtomicIdGenerator} implementation backed by Atomix
 * {@link AsyncAtomicCounter}.
 */
public class RaftIdGenerator implements AsyncAtomicIdGenerator {

    private static final long DEFAULT_BATCH_SIZE = 1000;
    private final AsyncAtomicCounter counter;
    private final long batchSize;
    private final AtomicLong delta = new AtomicLong();
    private CompletableFuture<Long> reserveFuture;
    private long base;

    public RaftIdGenerator(AsyncAtomicCounter counter) {
        this(counter, DEFAULT_BATCH_SIZE);
    }

    RaftIdGenerator(AsyncAtomicCounter counter, long batchSize) {
        this.counter = counter;
        this.batchSize = batchSize;
    }

    @Override
    public String name() {
        return counter.name();
    }

    @Override
    public synchronized CompletableFuture<Long> nextId() {
        long nextDelta = delta.incrementAndGet();
        if ((base == 0 && reserveFuture == null) || nextDelta > batchSize) {
            delta.set(0);
            long delta = this.delta.incrementAndGet();
            return reserve().thenApply(base -> base + delta);
        } else {
            return reserveFuture.thenApply(base -> base + nextDelta);
        }
    }

    private CompletableFuture<Long> reserve() {
        if (reserveFuture == null || reserveFuture.isDone()) {
            reserveFuture = counter.getAndAdd(batchSize);
        } else {
            reserveFuture = reserveFuture.thenCompose(v -> counter.getAndAdd(batchSize));
        }
        reserveFuture = reserveFuture.thenApply(base -> {
            this.base = base;
            return base;
        });
        return reserveFuture;
    }

    @Override
    public CompletableFuture<Void> close() {
        return counter.close();
    }
}
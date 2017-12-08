package org.logstash.cluster.primitives.counter;

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.AsyncPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.counter.impl.BlockingAtomicCounter;

/**
 * An async atomic counter dispenses monotonically increasing values.
 */
public interface AsyncAtomicCounter extends AsyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.COUNTER;
    }

    /**
     * Atomically increment by one the current value.
     * @return updated value
     */
    CompletableFuture<Long> incrementAndGet();

    /**
     * Atomically increment by one the current value.
     * @return previous value
     */
    CompletableFuture<Long> getAndIncrement();

    /**
     * Atomically adds the given value to the current value.
     * @param delta the value to add
     * @return previous value
     */
    CompletableFuture<Long> getAndAdd(long delta);

    /**
     * Atomically adds the given value to the current value.
     * @param delta the value to add
     * @return updated value
     */
    CompletableFuture<Long> addAndGet(long delta);

    /**
     * Returns the current value of the counter without modifying it.
     * @return current value
     */
    CompletableFuture<Long> get();

    /**
     * Atomically sets the given value to the current value.
     * @param value new value
     * @return future void
     */
    CompletableFuture<Void> set(long value);

    /**
     * Atomically sets the given counter to the updated value if the current value is the expected value, otherwise
     * no change occurs.
     * @param expectedValue the expected current value of the counter
     * @param updateValue the new value to be set
     * @return true if the update occurred and the expected value was equal to the current value, false otherwise
     */
    CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue);

    /**
     * Returns a new {@link AtomicCounter} that is backed by this instance.
     * @param timeoutMillis timeout duration for the returned ConsistentMap operations
     * @return new {@code ConsistentMap} instance
     */
    default AtomicCounter asAtomicCounter(long timeoutMillis) {
        return new BlockingAtomicCounter(this, timeoutMillis);
    }

    /**
     * Returns a new {@link AtomicCounter} that is backed by this instance and with a default operation timeout.
     * @return new {@code ConsistentMap} instance
     */
    default AtomicCounter asAtomicCounter() {
        return new BlockingAtomicCounter(this, DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }
}
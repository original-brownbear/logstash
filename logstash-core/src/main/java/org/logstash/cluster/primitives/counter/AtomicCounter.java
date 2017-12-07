package org.logstash.cluster.primitives.counter;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.SyncPrimitive;

/**
 * Distributed version of java.util.concurrent.atomic.AtomicLong.
 */
public interface AtomicCounter extends SyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.COUNTER;
    }

    /**
     * Atomically increment by one the current value.
     * @return updated value
     */
    long incrementAndGet();

    /**
     * Atomically increment by one the current value.
     * @return previous value
     */
    long getAndIncrement();

    /**
     * Atomically adds the given value to the current value.
     * @param delta the value to add
     * @return previous value
     */
    long getAndAdd(long delta);

    /**
     * Atomically adds the given value to the current value.
     * @param delta the value to add
     * @return updated value
     */
    long addAndGet(long delta);

    /**
     * Atomically sets the given value to the current value.
     * @param value the value to set
     */
    void set(long value);

    /**
     * Atomically sets the given counter to the updated value if the current value is the expected value, otherwise
     * no change occurs.
     * @param expectedValue the expected current value of the counter
     * @param updateValue the new value to be set
     * @return true if the update occurred and the expected value was equal to the current value, false otherwise
     */
    boolean compareAndSet(long expectedValue, long updateValue);

    /**
     * Returns the current value of the counter without modifying it.
     * @return current value
     */
    long get();
}

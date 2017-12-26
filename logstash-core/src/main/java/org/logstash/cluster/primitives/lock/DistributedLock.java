package org.logstash.cluster.primitives.lock;

import java.time.Duration;
import java.util.Optional;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.SyncPrimitive;
import org.logstash.cluster.time.Version;

/**
 * Asynchronous lock primitive.
 */
public interface DistributedLock extends SyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.LOCK;
    }

    /**
     * Acquires the lock, blocking until it's available.
     * @return the acquired lock version
     */
    Version lock();

    /**
     * Attempts to acquire the lock.
     * @return indicates whether the lock was acquired
     */
    Optional<Version> tryLock();

    /**
     * Attempts to acquire the lock for a specified amount of time.
     * @param timeout the timeout after which to give up attempting to acquire the lock
     * @return indicates whether the lock was acquired
     */
    Optional<Version> tryLock(Duration timeout);

    /**
     * Unlocks the lock.
     */
    void unlock();

}

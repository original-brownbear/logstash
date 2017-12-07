package org.logstash.cluster.primitives.lock;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.AsyncPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.lock.impl.BlockingDistributedLock;
import org.logstash.cluster.time.Version;

/**
 * Asynchronous lock primitive.
 */
public interface AsyncDistributedLock extends AsyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.LOCK;
    }

    /**
     * Acquires the lock, blocking until it's available.
     * @return future to be completed once the lock has been acquired
     */
    CompletableFuture<Version> lock();

    /**
     * Attempts to acquire the lock.
     * @return future to be completed with a boolean indicating whether the lock was acquired
     */
    CompletableFuture<Optional<Version>> tryLock();

    /**
     * Attempts to acquire the lock for a specified amount of time.
     * @param timeout the timeout after which to give up attempting to acquire the lock
     * @return future to be completed with a boolean indicating whether the lock was acquired
     */
    CompletableFuture<Optional<Version>> tryLock(Duration timeout);

    /**
     * Unlocks the lock.
     * @return future to be completed once the lock has been released
     */
    CompletableFuture<Void> unlock();

    /**
     * Returns a new {@link DistributedLock} that is backed by this instance.
     * @param timeoutMillis timeout duration for the returned DistributedLock operations
     * @return new {@code DistributedLock} instance
     */
    default DistributedLock asDistributedLock(long timeoutMillis) {
        return new BlockingDistributedLock(this, timeoutMillis);
    }

    /**
     * Returns a new {@link DistributedLock} that is backed by this instance and with a default operation timeout.
     * @return new {@code DistributedLock} instance
     */
    default DistributedLock asDistributedLock() {
        return new BlockingDistributedLock(this, DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }
}

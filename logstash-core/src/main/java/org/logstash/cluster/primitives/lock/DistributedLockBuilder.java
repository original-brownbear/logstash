package org.logstash.cluster.primitives.lock;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for AtomicIdGenerator.
 */
public abstract class DistributedLockBuilder
    extends DistributedPrimitiveBuilder<DistributedLockBuilder, DistributedLock, AsyncDistributedLock> {

    private Duration lockTimeout = Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS);

    public DistributedLockBuilder() {
        super(DistributedPrimitive.Type.LOCK);
    }

    /**
     * Sets the lock timeout in milliseconds.
     * @param lockTimeoutMillis the lock timeout in milliseconds
     * @return leader elector builder
     */
    public DistributedLockBuilder withLockTimeout(long lockTimeoutMillis) {
        return withLockTimeout(Duration.ofMillis(lockTimeoutMillis));
    }

    /**
     * Sets the lock timeout.
     * @param lockTimeout the lock timeout
     * @return leader elector builder
     */
    public DistributedLockBuilder withLockTimeout(Duration lockTimeout) {
        this.lockTimeout = Preconditions.checkNotNull(lockTimeout);
        return this;
    }

    /**
     * Sets the lock timeout.
     * @param lockTimeout the lock timeout
     * @param timeUnit the timeout time unit
     * @return leader elector builder
     */
    public DistributedLockBuilder withLockTimeout(long lockTimeout, TimeUnit timeUnit) {
        return withLockTimeout(Duration.ofMillis(timeUnit.toMillis(lockTimeout)));
    }

    /**
     * Returns the lock timeout.
     * @return the lock timeout
     */
    public Duration lockTimeout() {
        return lockTimeout;
    }

    @Override
    public DistributedLock build() {
        return buildAsync().asDistributedLock();
    }
}

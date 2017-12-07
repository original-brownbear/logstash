package org.logstash.cluster.primitives.generator;

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.AsyncPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.generator.impl.BlockingAtomicIdGenerator;

/**
 * An async ID generator for generating globally unique numbers.
 */
public interface AsyncAtomicIdGenerator extends AsyncPrimitive {

    @Override
    default DistributedPrimitive.Type primitiveType() {
        return DistributedPrimitive.Type.ID_GENERATOR;
    }

    /**
     * Returns the next globally unique numeric ID.
     * @return a future to be completed with the next globally unique identifier
     */
    CompletableFuture<Long> nextId();

    /**
     * Returns a new {@link AtomicIdGenerator} that is backed by this instance.
     * @param timeoutMillis timeout duration for the returned ConsistentMap operations
     * @return new {@code AtomicIdGenerator} instance
     */
    default AtomicIdGenerator asAtomicIdGenerator(long timeoutMillis) {
        return new BlockingAtomicIdGenerator(this, timeoutMillis);
    }

    /**
     * Returns a new {@link AtomicIdGenerator} that is backed by this instance and with a default operation timeout.
     * @return new {@code AtomicIdGenerator} instance
     */
    default AtomicIdGenerator asAtomicIdGenerator() {
        return new BlockingAtomicIdGenerator(this, DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }
}

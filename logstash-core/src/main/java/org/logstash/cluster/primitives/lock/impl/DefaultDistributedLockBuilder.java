package org.logstash.cluster.primitives.lock.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.lock.AsyncDistributedLock;
import org.logstash.cluster.primitives.lock.DistributedLockBuilder;

/**
 * Default distributed lock builder implementation.
 */
public class DefaultDistributedLockBuilder extends DistributedLockBuilder {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultDistributedLockBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncDistributedLock buildAsync() {
        return primitiveCreator.newAsyncDistributedLock(name(), lockTimeout());
    }
}

package org.logstash.cluster.primitives.counter.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.counter.AsyncAtomicCounter;
import org.logstash.cluster.primitives.counter.AtomicCounterBuilder;

/**
 * Default implementation of AtomicCounterBuilder.
 */
public class DefaultAtomicCounterBuilder extends AtomicCounterBuilder {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultAtomicCounterBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncAtomicCounter buildAsync() {
        return primitiveCreator.newAsyncCounter(name());
    }
}

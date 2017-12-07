package org.logstash.cluster.primitives.generator.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.generator.AsyncAtomicIdGenerator;
import org.logstash.cluster.primitives.generator.AtomicIdGeneratorBuilder;

/**
 * Default implementation of AtomicIdGeneratorBuilder.
 */
public class DefaultAtomicIdGeneratorBuilder extends AtomicIdGeneratorBuilder {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultAtomicIdGeneratorBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncAtomicIdGenerator buildAsync() {
        return primitiveCreator.newAsyncIdGenerator(name());
    }
}

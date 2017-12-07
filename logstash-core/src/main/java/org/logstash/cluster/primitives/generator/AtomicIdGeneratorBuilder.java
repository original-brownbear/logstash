package org.logstash.cluster.primitives.generator;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for AtomicIdGenerator.
 */
public abstract class AtomicIdGeneratorBuilder
    extends DistributedPrimitiveBuilder<AtomicIdGeneratorBuilder, AtomicIdGenerator, AsyncAtomicIdGenerator> {
    public AtomicIdGeneratorBuilder() {
        super(DistributedPrimitive.Type.ID_GENERATOR);
    }

    @Override
    public AtomicIdGenerator build() {
        return buildAsync().asAtomicIdGenerator();
    }
}

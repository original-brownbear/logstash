package org.logstash.cluster.primitives.counter;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for AtomicCounter.
 */
public abstract class AtomicCounterBuilder
    extends DistributedPrimitiveBuilder<AtomicCounterBuilder, AtomicCounter, AsyncAtomicCounter> {
    public AtomicCounterBuilder() {
        super(DistributedPrimitive.Type.COUNTER);
    }

    @Override
    public AtomicCounter build() {
        return buildAsync().asAtomicCounter();
    }
}

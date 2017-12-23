package org.logstash.cluster.primitives.value;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for constructing new AtomicValue instances.
 * @param <V> atomic value type
 */
public abstract class AtomicValueBuilder<V>
    extends DistributedPrimitiveBuilder<AtomicValueBuilder<V>, AtomicValue<V>, AsyncAtomicValue<V>> {

    public AtomicValueBuilder() {
        super(DistributedPrimitive.Type.VALUE);
    }

    @Override
    public AtomicValue<V> build() {
        return buildAsync().asAtomicValue();
    }
}

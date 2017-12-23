package org.logstash.cluster.primitives.value.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.primitives.value.AtomicValueBuilder;

/**
 * Default implementation of AtomicValueBuilder.
 * @param <V> value type
 */
public class DefaultAtomicValueBuilder<V> extends AtomicValueBuilder<V> {
    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultAtomicValueBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncAtomicValue<V> buildAsync() {
        return primitiveCreator.newAsyncAtomicValue(name(), serializer());
    }
}

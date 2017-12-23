package org.logstash.cluster.primitives.map.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.map.AsyncAtomicCounterMap;
import org.logstash.cluster.primitives.map.AtomicCounterMapBuilder;

/**
 * Default {@code AtomicCounterMapBuilder}.
 */
public class DefaultAtomicCounterMapBuilder<K> extends AtomicCounterMapBuilder<K> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultAtomicCounterMapBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncAtomicCounterMap<K> buildAsync() {
        return primitiveCreator.newAsyncAtomicCounterMap(name(), serializer());
    }
}

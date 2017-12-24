package org.logstash.cluster.primitives.map;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for AtomicCounterMap.
 */
public abstract class AtomicCounterMapBuilder<K>
    extends DistributedPrimitiveBuilder<AtomicCounterMapBuilder<K>, AtomicCounterMap<K>, AsyncAtomicCounterMap<K>> {
    public AtomicCounterMapBuilder() {
        super(DistributedPrimitive.Type.COUNTER_MAP);
    }

    @Override
    public AtomicCounterMap<K> build() {
        return buildAsync().asAtomicCounterMap();
    }
}

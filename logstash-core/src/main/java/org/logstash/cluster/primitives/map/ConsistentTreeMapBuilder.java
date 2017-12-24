package org.logstash.cluster.primitives.map;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for {@link ConsistentTreeMap}.
 */
public abstract class ConsistentTreeMapBuilder<K, V>
    extends DistributedPrimitiveBuilder<ConsistentTreeMapBuilder<K, V>, ConsistentTreeMap<K, V>, AsyncConsistentTreeMap<K, V>> {

    public ConsistentTreeMapBuilder() {
        super(DistributedPrimitive.Type.CONSISTENT_TREEMAP);
    }

    @Override
    public ConsistentTreeMap<K, V> build() {
        return buildAsync().asTreeMap();
    }
}

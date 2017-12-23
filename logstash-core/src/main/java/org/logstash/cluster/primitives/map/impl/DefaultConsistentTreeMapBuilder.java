package org.logstash.cluster.primitives.map.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.map.AsyncConsistentTreeMap;
import org.logstash.cluster.primitives.map.ConsistentTreeMapBuilder;

/**
 * Default {@link AsyncConsistentTreeMap} builder.
 * @param <V> type for map value
 */
public class DefaultConsistentTreeMapBuilder<K, V> extends ConsistentTreeMapBuilder<K, V> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultConsistentTreeMapBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncConsistentTreeMap<K, V> buildAsync() {
        return primitiveCreator.newAsyncConsistentTreeMap(name(), serializer());
    }
}

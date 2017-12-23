package org.logstash.cluster.primitives.map.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.DistributedPrimitives;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.primitives.map.ConsistentMapBuilder;

/**
 * Default {@link AsyncConsistentMap} builder.
 * @param <K> type for map key
 * @param <V> type for map value
 */
public class DefaultConsistentMapBuilder<K, V> extends ConsistentMapBuilder<K, V> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultConsistentMapBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncConsistentMap<K, V> buildAsync() {
        AsyncConsistentMap<K, V> map = primitiveCreator.newAsyncConsistentMap(name(), serializer());
        map = nullValues() ? map : DistributedPrimitives.newNotNullMap(map);
        map = relaxedReadConsistency() ? DistributedPrimitives.newCachingMap(map) : map;
        map = readOnly() ? DistributedPrimitives.newUnmodifiableMap(map) : map;
        return map;
    }
}

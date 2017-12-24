package org.logstash.cluster.primitives.map;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for {@link ConsistentMap} instances.
 * @param <K> type for map key
 * @param <V> type for map value
 */
public abstract class ConsistentMapBuilder<K, V>
    extends DistributedPrimitiveBuilder<ConsistentMapBuilder<K, V>, ConsistentMap<K, V>, AsyncConsistentMap<K, V>> {

    private boolean nullValues = false;

    public ConsistentMapBuilder() {
        super(DistributedPrimitive.Type.CONSISTENT_MAP);
    }

    /**
     * Enables null values in the map.
     * @return this builder
     */
    public ConsistentMapBuilder<K, V> withNullValues() {
        nullValues = true;
        return this;
    }

    /**
     * Returns whether null values are supported by the map.
     * @return {@code true} if null values are supported; {@code false} otherwise
     */
    public boolean nullValues() {
        return nullValues;
    }

    @Override
    public ConsistentMap<K, V> build() {
        return buildAsync().asConsistentMap();
    }
}

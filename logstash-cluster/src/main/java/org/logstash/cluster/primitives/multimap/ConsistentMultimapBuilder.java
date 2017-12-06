package org.logstash.cluster.primitives.multimap;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * A builder class for {@code AsyncConsistentMultimap}.
 */
public abstract class ConsistentMultimapBuilder<K, V>
    extends DistributedPrimitiveBuilder<ConsistentMultimapBuilder<K, V>, ConsistentMultimap<K, V>, AsyncConsistentMultimap<K, V>> {

    public ConsistentMultimapBuilder() {
        super(DistributedPrimitive.Type.CONSISTENT_MULTIMAP);
    }

    @Override
    public ConsistentMultimap<K, V> build() {
        return buildAsync().asMultimap();
    }
}

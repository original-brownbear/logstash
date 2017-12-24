package org.logstash.cluster.primitives.multimap.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.multimap.AsyncConsistentMultimap;
import org.logstash.cluster.primitives.multimap.ConsistentMultimapBuilder;

/**
 * Default {@link AsyncConsistentMultimap} builder.
 */
public class DefaultConsistentMultimapBuilder<K, V> extends ConsistentMultimapBuilder<K, V> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultConsistentMultimapBuilder(
        DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncConsistentMultimap<K, V> buildAsync() {
        return primitiveCreator.newAsyncConsistentSetMultimap(name(), serializer());
    }
}

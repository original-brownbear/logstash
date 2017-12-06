package org.logstash.cluster.primitives.set;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Builder for distributed set.
 * @param <E> type set elements.
 */
public abstract class DistributedSetBuilder<E> extends DistributedPrimitiveBuilder<DistributedSetBuilder<E>, DistributedSet<E>, AsyncDistributedSet<E>> {
    public DistributedSetBuilder() {
        super(DistributedPrimitive.Type.SET);
    }

    @Override
    public DistributedSet<E> build() {
        return buildAsync().asDistributedSet();
    }
}

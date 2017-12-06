package org.logstash.cluster.primitives.set.impl;

import java.util.function.Supplier;
import org.logstash.cluster.primitives.map.ConsistentMapBuilder;
import org.logstash.cluster.primitives.set.AsyncDistributedSet;
import org.logstash.cluster.primitives.set.DistributedSetBuilder;
import org.logstash.cluster.serializer.Serializer;

/**
 * Default distributed set builder.
 * @param <E> type for set elements
 */
public class DefaultDistributedSetBuilder<E> extends DistributedSetBuilder<E> {

    private ConsistentMapBuilder<E, Boolean> mapBuilder;

    public DefaultDistributedSetBuilder(Supplier<ConsistentMapBuilder<E, Boolean>> mapBuilderSupplier) {
        this.mapBuilder = mapBuilderSupplier.get();
    }

    @Override
    public DistributedSetBuilder<E> withName(String name) {
        mapBuilder.withName(name);
        return this;
    }

    @Override
    public DistributedSetBuilder<E> withSerializer(Serializer serializer) {
        mapBuilder.withSerializer(serializer);
        return this;
    }

    @Override
    public DistributedSetBuilder<E> withUpdatesDisabled() {
        mapBuilder.withUpdatesDisabled();
        return this;
    }

    @Override
    public DistributedSetBuilder<E> withRelaxedReadConsistency() {
        mapBuilder.withRelaxedReadConsistency();
        return this;
    }

    @Override
    public boolean readOnly() {
        return mapBuilder.readOnly();
    }

    @Override
    public boolean relaxedReadConsistency() {
        return mapBuilder.relaxedReadConsistency();
    }

    @Override
    public Serializer serializer() {
        return mapBuilder.serializer();
    }

    @Override
    public String name() {
        return mapBuilder.name();
    }

    @Override
    public AsyncDistributedSet<E> buildAsync() {
        return new DelegatingAsyncDistributedSet<>(mapBuilder.buildAsync());
    }
}

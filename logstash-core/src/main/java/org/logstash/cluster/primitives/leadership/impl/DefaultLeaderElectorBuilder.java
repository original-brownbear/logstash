package org.logstash.cluster.primitives.leadership.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.leadership.AsyncLeaderElector;
import org.logstash.cluster.primitives.leadership.LeaderElectorBuilder;

/**
 * Default implementation of {@code LeaderElectorBuilder}.
 */
public class DefaultLeaderElectorBuilder<T> extends LeaderElectorBuilder<T> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultLeaderElectorBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncLeaderElector<T> buildAsync() {
        return primitiveCreator.newAsyncLeaderElector(name(), serializer(), electionTimeout());
    }
}

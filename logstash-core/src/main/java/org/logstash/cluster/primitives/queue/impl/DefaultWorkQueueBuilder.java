package org.logstash.cluster.primitives.queue.impl;

import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.queue.WorkQueueBuilder;

/**
 * Default work queue builder implementation.
 */
public class DefaultWorkQueueBuilder<E> extends WorkQueueBuilder<E> {

    private final DistributedPrimitiveCreator primitiveCreator;

    public DefaultWorkQueueBuilder(DistributedPrimitiveCreator primitiveCreator) {
        this.primitiveCreator = primitiveCreator;
    }

    @Override
    public AsyncWorkQueue<E> buildAsync() {
        return primitiveCreator.newAsyncWorkQueue(name(), serializer());
    }
}

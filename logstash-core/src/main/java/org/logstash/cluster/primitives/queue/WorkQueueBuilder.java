package org.logstash.cluster.primitives.queue;

import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveBuilder;

/**
 * Work queue builder.
 */
public abstract class WorkQueueBuilder<E> extends DistributedPrimitiveBuilder<WorkQueueBuilder<E>, WorkQueue<E>, AsyncWorkQueue<E>> {

    public WorkQueueBuilder() {
        super(DistributedPrimitive.Type.WORK_QUEUE);
    }

    @Override
    public WorkQueue<E> build() {
        return buildAsync().asWorkQueue();
    }
}

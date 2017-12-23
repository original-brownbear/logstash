package org.logstash.cluster.primitives.queue.impl;

import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Atomix work queue events.
 */
public enum RaftWorkQueueEvents implements EventType {
    TASK_AVAILABLE("taskAvailable");

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 50)
        .build(RaftWorkQueueEvents.class.getSimpleName());
    private final String id;

    RaftWorkQueueEvents(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }
}

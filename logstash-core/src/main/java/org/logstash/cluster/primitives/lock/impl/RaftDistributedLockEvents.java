package org.logstash.cluster.primitives.lock.impl;

import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Raft value events.
 */
public enum RaftDistributedLockEvents implements EventType {
    LOCK("lock"),
    FAIL("fail");

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 50)
        .register(LockEvent.class)
        .register(byte[].class)
        .build(RaftDistributedLockEvents.class.getSimpleName());
    private final String id;

    RaftDistributedLockEvents(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

}

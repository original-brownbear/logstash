package org.logstash.cluster.primitives.value.impl;

import org.logstash.cluster.primitives.value.AtomicValueEvent;
import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Raft value events.
 */
public enum RaftAtomicValueEvents implements EventType {
    CHANGE("change");

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 50)
        .register(AtomicValueEvent.class)
        .register(AtomicValueEvent.Type.class)
        .register(byte[].class)
        .build(RaftAtomicValueEvents.class.getSimpleName());
    private final String id;

    RaftAtomicValueEvents(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

}

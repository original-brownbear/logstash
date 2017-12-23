package org.logstash.cluster.primitives.map.impl;

import org.logstash.cluster.primitives.map.MapEvent;
import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Raft consistent map events.
 */
public enum RaftConsistentMapEvents implements EventType {
    CHANGE("change");

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 50)
        .register(MapEvent.class)
        .register(MapEvent.Type.class)
        .register(byte[].class)
        .build(RaftConsistentMapEvents.class.getSimpleName());
    private final String id;

    RaftConsistentMapEvents(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

}

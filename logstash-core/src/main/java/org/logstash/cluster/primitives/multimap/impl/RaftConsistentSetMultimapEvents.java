package org.logstash.cluster.primitives.multimap.impl;

import org.logstash.cluster.primitives.multimap.MultimapEvent;
import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Consistent set multimap events.
 */
public enum RaftConsistentSetMultimapEvents implements EventType {
    CHANGE("change");

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 50)
        .register(MultimapEvent.class)
        .register(MultimapEvent.Type.class)
        .register(byte[].class)
        .build(RaftConsistentSetMultimapEvents.class.getSimpleName());
    private final String id;

    RaftConsistentSetMultimapEvents(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }
}

package org.logstash.cluster.primitives.leadership.impl;

import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.primitives.leadership.Leader;
import org.logstash.cluster.primitives.leadership.Leadership;
import org.logstash.cluster.primitives.leadership.LeadershipEvent;
import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Atomix leader elector events.
 */
public enum RaftLeaderElectorEvents implements EventType {
    CHANGE("change");

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 50)
        .register(NodeId.class)
        .register(Leadership.class)
        .register(Leader.class)
        .register(LeadershipEvent.class)
        .register(LeadershipEvent.Type.class)
        .build(RaftLeaderElectorEvents.class.getSimpleName());
    private final String id;

    RaftLeaderElectorEvents(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }
}

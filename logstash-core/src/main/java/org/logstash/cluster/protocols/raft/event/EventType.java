package org.logstash.cluster.protocols.raft.event;

import org.logstash.cluster.protocols.raft.event.impl.DefaultEventType;
import org.logstash.cluster.utils.Identifier;

/**
 * Raft event identifier.
 */
public interface EventType extends Identifier<String> {

    /**
     * Creates a new Raft event identifier.
     * @param name the event name
     * @return the event identifier
     */
    static EventType from(String name) {
        return new DefaultEventType(name);
    }
}

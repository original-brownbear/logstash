package org.logstash.cluster.protocols.raft.event.impl;

import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Default Raft event identifier.
 */
public class DefaultEventType extends AbstractIdentifier<String> implements EventType {
    private DefaultEventType() {
    }

    public DefaultEventType(String value) {
        super(value);
    }
}

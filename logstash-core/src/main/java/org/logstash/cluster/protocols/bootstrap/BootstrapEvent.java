package org.logstash.cluster.protocols.bootstrap;

import org.logstash.cluster.event.AbstractEvent;

/**
 * Bootstrap event.
 */
public class BootstrapEvent<T> extends AbstractEvent<BootstrapEvent.Type, T> {

    public BootstrapEvent(Type type, T subject) {
        super(type, subject);
    }

    public BootstrapEvent(Type type, T subject, long time) {
        super(type, subject, time);
    }

    /**
     * Failure detection event type.
     */
    public enum Type {
        BACKUP,
    }
}

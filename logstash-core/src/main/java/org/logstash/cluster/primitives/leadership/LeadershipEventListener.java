package org.logstash.cluster.primitives.leadership;

import org.logstash.cluster.event.EventListener;

/**
 * Entity capable of receiving leader elector events.
 */
public interface LeadershipEventListener<T> extends EventListener<LeadershipEvent<T>> {
}

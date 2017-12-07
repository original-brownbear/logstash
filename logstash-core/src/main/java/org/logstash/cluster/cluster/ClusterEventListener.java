package org.logstash.cluster.cluster;

import org.logstash.cluster.event.EventListener;

/**
 * Entity capable of receiving device cluster-related events.
 */
public interface ClusterEventListener extends EventListener<ClusterEvent> {
}

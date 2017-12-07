package org.logstash.cluster.messaging;

import org.logstash.cluster.utils.Managed;

/**
 * Managed cluster event service.
 */
public interface ManagedClusterEventService extends ClusterEventService, Managed<ClusterEventService> {
}

package org.logstash.cluster.cluster;

import org.logstash.cluster.utils.Managed;

/**
 * Managed cluster.
 */
public interface ManagedClusterService extends ClusterService, Managed<ClusterService> {
}

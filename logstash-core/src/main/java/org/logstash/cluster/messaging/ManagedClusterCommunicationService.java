package org.logstash.cluster.messaging;

import org.logstash.cluster.utils.Managed;

/**
 * Managed cluster communicator.
 */
public interface ManagedClusterCommunicationService extends ClusterCommunicationService, Managed<ClusterCommunicationService> {
}

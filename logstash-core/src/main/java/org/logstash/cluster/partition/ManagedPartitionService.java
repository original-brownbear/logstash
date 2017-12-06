package org.logstash.cluster.partition;

import org.logstash.cluster.utils.Managed;

/**
 * Managed partition service.
 */
public interface ManagedPartitionService extends PartitionService, Managed<PartitionService> {
}

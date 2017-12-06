package org.logstash.cluster.partition;

import org.logstash.cluster.utils.Managed;

/**
 * Managed partition.
 */
public interface ManagedPartition extends Partition, Managed<Partition> {
}

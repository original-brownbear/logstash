package org.logstash.cluster.partition;

import org.logstash.cluster.primitives.PrimitiveService;

/**
 * Atomix partition.
 */
public interface Partition extends PrimitiveService {

  /**
   * Returns the partition identifier.
   *
   * @return the partition identifier
   */
  PartitionId id();

}

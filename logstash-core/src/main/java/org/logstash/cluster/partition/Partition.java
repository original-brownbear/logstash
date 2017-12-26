package org.logstash.cluster.partition;

import org.logstash.cluster.primitives.PrimitiveService;

/**
 * Partition.
 */
public interface Partition extends PrimitiveService {

    /**
     * Returns the partition identifier.
     * @return the partition identifier
     */
    PartitionId id();

}

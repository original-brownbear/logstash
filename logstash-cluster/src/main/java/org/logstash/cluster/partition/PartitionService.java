package org.logstash.cluster.partition;

import java.util.Collection;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;

/**
 * Partition service.
 */
public interface PartitionService {

    /**
     * Returns a partition by ID.
     * @param partitionId the partition identifier
     * @return the partition or {@code null} if no partition with the given identifier exists
     */
    default Partition getPartition(int partitionId) {
        return getPartition(PartitionId.from(partitionId));
    }

    /**
     * Returns a partition by ID.
     * @param partitionId the partition identifier
     * @return the partition or {@code null} if no partition with the given identifier exists
     * @throws NullPointerException if the partition identifier is {@code null}
     */
    Partition getPartition(PartitionId partitionId);

    /**
     * Returns the primitive creator for the given partition.
     * @param partitionId the partition identifier
     * @return the primitive creator for the given partition
     */
    default DistributedPrimitiveCreator getPrimitiveCreator(int partitionId) {
        return getPrimitiveCreator(PartitionId.from(partitionId));
    }

    /**
     * Returns the primitive creator for the given partition.
     * @param partitionId the partition identifier
     * @return the primitive creator for the given partition
     * @throws NullPointerException if the partition identifier is {@code null}
     */
    DistributedPrimitiveCreator getPrimitiveCreator(PartitionId partitionId);

    /**
     * Returns a collection of all partitions.
     * @return a collection of all partitions
     */
    Collection<Partition> getPartitions();

}

package org.logstash.cluster.partition;

import com.google.common.base.Preconditions;
import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * {@link PartitionMetadata} identifier.
 */
public class PartitionId extends AbstractIdentifier<Integer> implements Comparable<PartitionId> {

    /**
     * Creates a partition identifier from an integer.
     * @param id input integer
     */
    public PartitionId(int id) {
        super(id);
        Preconditions.checkArgument(id >= 0, "partition id must be non-negative");
    }

    /**
     * Creates a partition identifier from an integer.
     * @param id input integer
     * @return partition identification
     */
    public static PartitionId from(int id) {
        return new PartitionId(id);
    }

    @Override
    public int compareTo(PartitionId that) {
        return Integer.compare(this.identifier, that.identifier);
    }
}

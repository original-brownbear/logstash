package org.logstash.cluster.partition;

import com.google.common.base.MoreObjects;
import java.util.Collection;
import java.util.Objects;
import org.logstash.cluster.cluster.NodeId;

/**
 * A partition or shard is a group of controller nodes that are work together to maintain state.
 * A ONOS cluster is typically made of of one or partitions over which the the data is partitioned.
 */
public class PartitionMetadata {
    private final PartitionId id;
    private final Collection<NodeId> members;

    public PartitionMetadata(final PartitionId id, final Collection<NodeId> members) {
        this.id = id;
        this.members = members;
    }

    /**
     * Returns the partition identifier.
     * @return partition identifier
     */
    public PartitionId id() {
        return id;
    }

    /**
     * Returns the controller nodes that are members of this partition.
     * @return collection of controller node identifiers
     */
    public Collection<NodeId> members() {
        return members;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, members);
    }

    @Override
    public boolean equals(final Object object) {
        if (object instanceof PartitionMetadata) {
            final PartitionMetadata partition = (PartitionMetadata) object;
            return partition.id.equals(id) && partition.members.equals(members);
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("members", members)
            .toString();
    }
}

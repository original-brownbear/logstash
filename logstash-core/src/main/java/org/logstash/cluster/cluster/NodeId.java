package org.logstash.cluster.cluster;

import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Controller cluster identity.
 */
public final class NodeId extends AbstractIdentifier<String> implements Comparable<NodeId> {

    /**
     * Constructor for serialization.
     */
    private NodeId() {
        super("");
    }

    /**
     * Creates a new cluster node identifier from the specified string.
     * @param id string identifier
     */
    public NodeId(String id) {
        super(id);
    }

    /**
     * Creates a new cluster node identifier from the specified string.
     * @param id string identifier
     * @return node id
     */
    public static NodeId from(String id) {
        return new NodeId(id);
    }

    @Override
    public int compareTo(NodeId that) {
        return identifier.compareTo(that.identifier);
    }
}

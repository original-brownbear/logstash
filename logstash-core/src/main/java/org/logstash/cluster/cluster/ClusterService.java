package org.logstash.cluster.cluster;

import java.util.Set;
import org.logstash.cluster.event.ListenerService;

/**
 * Service for obtaining information about the individual nodes within
 * the controller cluster.
 */
public interface ClusterService extends ListenerService<ClusterEvent, ClusterEventListener> {

    /**
     * Returns the local controller node.
     * @return local controller node
     */
    Node getLocalNode();

    /**
     * Returns the set of current cluster members.
     * @return set of cluster members
     */
    Set<Node> getNodes();

    /**
     * Returns the specified controller node.
     * @param nodeId controller node identifier
     * @return controller node
     */
    Node getNode(NodeId nodeId);

}

package org.logstash.cluster.messaging;

import com.google.common.collect.Maps;
import java.util.Map;
import org.logstash.cluster.cluster.NodeId;

/**
 * Test cluster communication service factory.
 */
public class TestClusterCommunicationServiceFactory {
    private final Map<NodeId, TestClusterCommunicationService> nodes = Maps.newConcurrentMap();

    /**
     * Creates a new cluster communication service for the given node.
     * @param localNodeId the node for which to create the service
     * @return the communication service for the given node
     */
    public ClusterCommunicationService newCommunicationService(NodeId localNodeId) {
        return new TestClusterCommunicationService(localNodeId, nodes);
    }
}

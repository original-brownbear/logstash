package org.logstash.cluster.cluster;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.impl.DefaultNode;

/**
 * Cluster metadata.
 */
public final class ClusterMetadata {

    private final String name;
    private final Node localNode;
    private final Collection<Node> bootstrapNodes;

    ClusterMetadata(final String name, final Node localNode,
        final Collection<Node> bootstrapNodes) {
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.localNode = ((DefaultNode) localNode).setType(bootstrapNodes.contains(localNode) ? Node.Type.CORE : Node.Type.CLIENT);
        this.bootstrapNodes = bootstrapNodes.stream()
            .map(node -> ((DefaultNode) node).setType(Node.Type.CORE))
            .collect(Collectors.toList());
    }

    /**
     * Returns a new cluster metadata builder.
     * @return a new cluster metadata builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the cluster name.
     * @return the cluster name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the local node.
     * @return the local node
     */
    public Node localNode() {
        return localNode;
    }

    /**
     * Returns the collection of bootstrap nodes.
     * @return the collection of bootstrap nodes
     */
    public Collection<Node> bootstrapNodes() {
        return bootstrapNodes;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("localNode", localNode)
            .toString();
    }

    /**
     * Cluster metadata builder.
     */
    public static class Builder implements org.logstash.cluster.utils.Builder<ClusterMetadata> {
        private static final String DEFAULT_CLUSTER_NAME = "atomix";
        protected String name = DEFAULT_CLUSTER_NAME;
        protected Node localNode;
        protected Collection<Node> bootstrapNodes;

        /**
         * Sets the cluster name.
         * @param name the cluster name
         * @return the cluster metadata builder
         * @throws NullPointerException if the name is null
         */
        public Builder withClusterName(final String name) {
            this.name = Preconditions.checkNotNull(name, "name cannot be null");
            return this;
        }

        /**
         * Sets the local node metadata.
         * @param localNode the local node metadata
         * @return the cluster metadata builder
         */
        public Builder withLocalNode(final Node localNode) {
            this.localNode = Preconditions.checkNotNull(localNode, "localNode cannot be null");
            return this;
        }

        /**
         * Sets the bootstrap nodes.
         * @param bootstrapNodes the nodes from which to bootstrap the cluster
         * @return the cluster metadata builder
         * @throws NullPointerException if the bootstrap nodes are {@code null}
         */
        public Builder withBootstrapNodes(final Node... bootstrapNodes) {
            return withBootstrapNodes(Arrays.asList(Preconditions.checkNotNull(bootstrapNodes)));
        }

        /**
         * Sets the bootstrap nodes.
         * @param bootstrapNodes the nodes from which to bootstrap the cluster
         * @return the cluster metadata builder
         * @throws NullPointerException if the bootstrap nodes are {@code null}
         */
        public Builder withBootstrapNodes(final Collection<Node> bootstrapNodes) {
            this.bootstrapNodes = Preconditions.checkNotNull(bootstrapNodes, "bootstrapNodes cannot be null");
            return this;
        }

        @Override
        public ClusterMetadata build() {
            return new ClusterMetadata(
                name,
                localNode,
                bootstrapNodes);
        }
    }
}

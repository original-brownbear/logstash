package org.logstash.cluster.cluster;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.logstash.cluster.cluster.impl.DefaultNode;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Represents a controller instance as a member in a cluster.
 */
public abstract class Node {

    private final NodeId id;
    private final Endpoint endpoint;

    protected Node(NodeId id, Endpoint endpoint) {
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
        this.endpoint = Preconditions.checkNotNull(endpoint, "endpoint cannot be null");
    }

    /**
     * Returns a new node builder.
     * @return a new node builder
     */
    public static Builder builder() {
        return new DefaultNode.Builder();
    }

    /**
     * Returns the instance identifier.
     * @return instance identifier
     */
    public NodeId id() {
        return id;
    }

    /**
     * Returns the node endpoint.
     * @return the node endpoint
     */
    public Endpoint endpoint() {
        return endpoint;
    }

    /**
     * Returns the node type.
     * @return the node type
     */
    public abstract Type type();

    /**
     * Returns the node state.
     * @return the node state
     */
    public abstract State state();

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof Node && ((Node) object).id.equals(id);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("endpoint", endpoint)
            .toString();
    }

    /**
     * Node type.
     */
    public enum Type {

        /**
         * Represents a core node.
         */
        CORE,

        /**
         * Represents a client node.
         */
        CLIENT,
    }

    /**
     * Represents the operational state of the instance.
     */
    public enum State {

        /**
         * Signifies that the instance is active and operating normally.
         */
        ACTIVE,

        /**
         * Signifies that the instance is inactive, which means either down or
         * up, but not operational.
         */
        INACTIVE,
    }

    /**
     * Node builder.
     */
    public abstract static class Builder implements org.logstash.cluster.utils.Builder<Node> {
        protected NodeId id;
        protected Endpoint endpoint;

        /**
         * Sets the node identifier.
         * @param id the node identifier
         * @return the node builder
         */
        public Builder withId(NodeId id) {
            this.id = Preconditions.checkNotNull(id, "id cannot be null");
            return this;
        }

        /**
         * Sets the node endpoint.
         * @param endpoint the node endpoint
         * @return the node builder
         */
        public Builder withEndpoint(Endpoint endpoint) {
            this.endpoint = Preconditions.checkNotNull(endpoint, "endpoint cannot be null");
            return this;
        }
    }
}

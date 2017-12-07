package org.logstash.cluster.cluster.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.Endpoint;

/**
 * Default cluster node.
 */
public class DefaultNode extends Node {
    private Type type = Type.CLIENT;
    private State state = State.INACTIVE;

    public DefaultNode(NodeId id, Endpoint endpoint) {
        super(id, endpoint);
    }

    /**
     * Updates the node type.
     */
    public DefaultNode setType(Type type) {
        this.type = type;
        return this;
    }

    @Override
    public Type type() {
        return type;
    }

    @Override
    public State state() {
        return state;
    }

    /**
     * Updates the node state.
     * @param state the node state
     */
    void setState(State state) {
        this.state = state;
    }

    /**
     * Default cluster node builder.
     */
    public static class Builder extends Node.Builder {
        protected static final int DEFAULT_PORT = 5679;

        @Override
        public Node build() {
            if (endpoint == null) {
                try {
                    endpoint = new Endpoint(InetAddress.getByName("127.0.0.1"), DEFAULT_PORT);
                } catch (UnknownHostException e) {
                    throw new IllegalStateException("Failed to instantiate address", e);
                }
            }
            return new DefaultNode(id, endpoint);
        }
    }
}

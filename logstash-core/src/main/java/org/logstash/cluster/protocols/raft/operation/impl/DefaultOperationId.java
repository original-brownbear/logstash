package org.logstash.cluster.protocols.raft.operation.impl;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.utils.AbstractIdentifier;

/**
 * Raft operation identifier.
 */
public class DefaultOperationId extends AbstractIdentifier<String> implements OperationId {
    private final OperationType type;

    protected DefaultOperationId() {
        this.type = null;
    }

    public DefaultOperationId(String id, OperationType type) {
        super(id);
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id())
            .add("type", type())
            .toString();
    }

    /**
     * Returns the operation type.
     * @return the operation type
     */
    @Override
    public OperationType type() {
        return type;
    }
}

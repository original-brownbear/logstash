package org.logstash.cluster.protocols.raft.operation;

import org.logstash.cluster.protocols.raft.operation.impl.DefaultOperationId;
import org.logstash.cluster.utils.Identifier;

/**
 * Raft operation identifier.
 */
public interface OperationId extends Identifier<String> {

    /**
     * Returns a new command operation identifier.
     * @param id the command identifier
     * @return the operation identifier
     */
    static OperationId command(String id) {
        return from(id, OperationType.COMMAND);
    }

    /**
     * Returns a new operation identifier.
     * @param id the operation name
     * @param type the operation type
     * @return the operation identifier
     */
    static OperationId from(String id, OperationType type) {
        return new DefaultOperationId(id, type);
    }

    /**
     * Returns a new query operation identifier.
     * @param id the query identifier
     * @return the operation identifier
     */
    static OperationId query(String id) {
        return from(id, OperationType.QUERY);
    }

    /**
     * Returns the operation type.
     * @return the operation type
     */
    OperationType type();
}

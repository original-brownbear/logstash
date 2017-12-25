package org.logstash.cluster.protocols.raft.operation;

/**
 * Raft operation type.
 */
public enum OperationType {
    /**
     * Command operation.
     */
    COMMAND,

    /**
     * Query operation.
     */
    QUERY,
}

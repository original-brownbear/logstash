package org.logstash.cluster.primitives.map.impl;

/**
 * Response enum for two phase commit rollback operation.
 */
public enum RollbackResult {
    /**
     * Signifies a successful rollback execution.
     */
    OK,

    /**
     * Signifies a failure due to unrecognized transaction identifier.
     */
    UNKNOWN_TRANSACTION_ID,
}

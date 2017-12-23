package org.logstash.cluster.primitives.map.impl;

/**
 * Response enum for two phase commit prepare operation.
 */
public enum PrepareResult {
    /**
     * Signifies a successful execution of the prepare operation.
     */
    OK,

    /**
     * Signifies some participants in a distributed prepare operation failed.
     */
    PARTIAL_FAILURE,

    /**
     * Signifies a failure to another transaction locking the underlying state.
     */
    CONCURRENT_TRANSACTION,

    /**
     * Signifies a optimistic lock failure. This can happen if underlying state has changed since it was last read.
     */
    OPTIMISTIC_LOCK_FAILURE,
}

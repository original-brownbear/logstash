package org.logstash.cluster.primitives.map.impl;

/**
 * Response enum for two phase commit operation.
 */
public enum CommitResult {
    /**
     * Signifies a successful commit execution.
     */
    OK,

    /**
     * Signifies a failure due to unrecognized transaction identifier.
     */
    UNKNOWN_TRANSACTION_ID,

    /**
     * Signifies a failure to get participants to agree to commit (during prepare stage).
     */
    FAILURE_TO_PREPARE,

    /**
     * Failure during commit phase.
     */
    FAILURE_DURING_COMMIT
}

package org.logstash.cluster.protocols.raft.proxy;

/**
 * Session recovery strategy.
 */
public enum RecoveryStrategy {
    /**
     * Indicates that the session should be recovered when lost.
     */
    RECOVER,

    /**
     * Indicates that the session should be closed when lost.
     */
    CLOSE,
}

package org.logstash.cluster.protocols.raft.service;

import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.protocols.raft.session.RaftSessions;
import org.logstash.cluster.time.LogicalClock;
import org.logstash.cluster.time.WallClock;

/**
 * State machine context.
 * <p>
 * The context is reflective of the current position and state of the Raft state machine. In particular,
 * it exposes the current approximate {@link ServiceContext#wallClock() time} and all open
 * {@link RaftSessions}.
 */
public interface ServiceContext {

    /**
     * Returns the state machine identifier.
     * @return The unique state machine identifier.
     */
    ServiceId serviceId();

    /**
     * Returns the state machine name.
     * @return The state machine name.
     */
    String serviceName();

    /**
     * Returns the state machine type.
     * @return The state machine type.
     */
    ServiceType serviceType();

    /**
     * Returns the current state machine index.
     * <p>
     * The state index is indicative of the index of the current operation
     * being applied to the server state machine. If a query is being applied,
     * the index of the last command applied will be used.
     * @return The current state machine index.
     */
    long currentIndex();

    /**
     * Returns the current operation type.
     * @return the current operation type
     */
    OperationType currentOperation();

    /**
     * Returns the state machine's logical clock.
     * @return The state machine's logical clock.
     */
    LogicalClock logicalClock();

    /**
     * Returns the state machine's wall clock.
     * @return The state machine's wall clock.
     */
    WallClock wallClock();

    /**
     * Returns the state machine sessions.
     * @return The state machine sessions.
     */
    RaftSessions sessions();

}

package org.logstash.cluster.protocols.raft.proxy;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.storage.buffer.HeapBytes;

/**
 * Raft proxy executor.
 */
public interface RaftProxyExecutor {

    /**
     * Returns the proxy session identifier.
     * @return The proxy session identifier
     */
    SessionId sessionId();

    /**
     * Returns the client proxy name.
     * @return The client proxy name.
     */
    String name();

    /**
     * Returns the client proxy type.
     * @return The client proxy type.
     */
    ServiceType serviceType();

    /**
     * Returns the session state.
     * @return The session state.
     */
    RaftProxy.State getState();

    /**
     * Registers a session state change listener.
     * @param listener The callback to call when the session state changes.
     */
    void addStateChangeListener(Consumer<RaftProxy.State> listener);

    /**
     * Removes a state change listener.
     * @param listener the state change listener to remove
     */
    void removeStateChangeListener(Consumer<RaftProxy.State> listener);

    /**
     * Executes an operation to the Raft cluster.
     * @param operationId the operation identifier
     * @return a completable future to be completed with the operation result
     * @throws NullPointerException if {@code command} is null
     */
    default CompletableFuture<byte[]> execute(OperationId operationId) {
        return execute(new RaftOperation(operationId, HeapBytes.EMPTY));
    }

    /**
     * Executes an operation to the cluster.
     * @param operation the operation to execute
     * @return a future to be completed with the operation result
     * @throws NullPointerException if {@code operation} is null
     */
    CompletableFuture<byte[]> execute(RaftOperation operation);

    /**
     * Executes an operation to the Raft cluster.
     * @param operationId the operation identifier
     * @param operation the operation to execute
     * @return a completable future to be completed with the operation result
     * @throws NullPointerException if {@code command} is null
     */
    default CompletableFuture<byte[]> execute(OperationId operationId, byte[] operation) {
        return execute(new RaftOperation(operationId, operation));
    }

    /**
     * Adds a session event listener.
     * @param listener the event listener to add
     */
    void addEventListener(Consumer<RaftEvent> listener);

    /**
     * Removes a session event listener.
     * @param listener the event listener to remove
     */
    void removeEventListener(Consumer<RaftEvent> listener);

}

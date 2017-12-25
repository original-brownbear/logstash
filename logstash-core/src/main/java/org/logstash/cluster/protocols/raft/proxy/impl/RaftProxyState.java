package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.Preconditions;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.SessionId;

/**
 * Client state.
 */
public final class RaftProxyState {
    private final String clientId;
    private final SessionId sessionId;
    private final String serviceName;
    private final ServiceType serviceType;
    private final long timeout;
    private final Set<Consumer<RaftProxy.State>> changeListeners = new CopyOnWriteArraySet<>();
    private volatile RaftProxy.State state = RaftProxy.State.CONNECTED;
    private volatile Long suspendedTime;
    private volatile long commandRequest;
    private volatile long commandResponse;
    private volatile long responseIndex;
    private volatile long eventIndex;

    RaftProxyState(String clientId, SessionId sessionId, String serviceName, ServiceType serviceType, long timeout) {
        this.clientId = clientId;
        this.sessionId = sessionId;
        this.serviceName = serviceName;
        this.serviceType = serviceType;
        this.timeout = timeout;
        this.responseIndex = sessionId.id();
        this.eventIndex = sessionId.id();
    }

    /**
     * Returns the client identifier.
     * @return The client identifier.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Returns the client session ID.
     * @return The client session ID.
     */
    public SessionId getSessionId() {
        return sessionId;
    }

    /**
     * Returns the session name.
     * @return The session name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Returns the session type.
     * @return The session type.
     */
    public ServiceType getServiceType() {
        return serviceType;
    }

    /**
     * Returns the session timeout.
     * @return The session timeout.
     */
    public long getSessionTimeout() {
        return timeout;
    }

    /**
     * Returns the session state.
     * @return The session state.
     */
    public RaftProxy.State getState() {
        return state;
    }

    /**
     * Updates the session state.
     * @param state The updates session state.
     */
    public void setState(RaftProxy.State state) {
        if (this.state != state) {
            this.state = state;
            if (state == RaftProxy.State.SUSPENDED) {
                if (suspendedTime == null) {
                    suspendedTime = System.currentTimeMillis();
                }
            } else {
                suspendedTime = null;
            }
            changeListeners.forEach(l -> l.accept(state));
        } else if (this.state == RaftProxy.State.SUSPENDED) {
            if (System.currentTimeMillis() - suspendedTime > timeout) {
                setState(RaftProxy.State.CLOSED);
            }
        }
    }

    /**
     * Registers a state change listener on the session manager.
     * @param listener The state change listener callback.
     */
    public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
        changeListeners.add(Preconditions.checkNotNull(listener));
    }

    /**
     * Removes a state change listener.
     * @param listener the listener to remove
     */
    public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
        changeListeners.remove(Preconditions.checkNotNull(listener));
    }

    /**
     * Returns the last command request sequence number for the session.
     * @return The last command request sequence number for the session.
     */
    public long getCommandRequest() {
        return commandRequest;
    }

    /**
     * Sets the last command request sequence number.
     * @param commandRequest The last command request sequence number.
     */
    public void setCommandRequest(long commandRequest) {
        this.commandRequest = commandRequest;
    }

    /**
     * Returns the next command request sequence number for the session.
     * @return The next command request sequence number for the session.
     */
    public long nextCommandRequest() {
        return ++commandRequest;
    }

    /**
     * Returns the last command sequence number for which a response has been received.
     * @return The last command sequence number for which a response has been received.
     */
    public long getCommandResponse() {
        return commandResponse;
    }

    /**
     * Sets the last command sequence number for which a response has been received.
     * @param commandResponse The last command sequence number for which a response has been received.
     */
    public void setCommandResponse(long commandResponse) {
        this.commandResponse = commandResponse;
    }

    /**
     * Returns the highest index for which a response has been received.
     * @return The highest index for which a command or query response has been received.
     */
    public long getResponseIndex() {
        return responseIndex;
    }

    /**
     * Sets the highest index for which a response has been received.
     * @param responseIndex The highest index for which a command or query response has been received.
     */
    public void setResponseIndex(long responseIndex) {
        this.responseIndex = Math.max(this.responseIndex, responseIndex);
    }

    /**
     * Returns the highest index for which an event has been received in sequence.
     * @return The highest index for which an event has been received in sequence.
     */
    public long getEventIndex() {
        return eventIndex;
    }

    /**
     * Sets the highest index for which an event has been received in sequence.
     * @param eventIndex The highest index for which an event has been received in sequence.
     */
    public void setEventIndex(long eventIndex) {
        this.eventIndex = eventIndex;
    }
}

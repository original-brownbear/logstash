package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.logstash.cluster.protocols.raft.service.RaftService;

/**
 * State machine registry.
 */
public class RaftServiceFactoryRegistry {
    private final Map<String, Supplier<RaftService>> stateMachines = new ConcurrentHashMap<>();

    /**
     * Returns the number of registered state machines.
     * @return The number of registered state machines.
     */
    public int size() {
        return stateMachines.size();
    }

    /**
     * Registers a new state machine type.
     * @param type The state machine type to register.
     * @param factory The state machine factory.
     * @return The state machine registry.
     */
    public RaftServiceFactoryRegistry register(String type, Supplier<RaftService> factory) {
        stateMachines.put(Preconditions.checkNotNull(type, "type cannot be null"), Preconditions.checkNotNull(factory, "factory cannot be null"));
        return this;
    }

    /**
     * Unregisters the given state machine type.
     * @param type The state machine type to unregister.
     * @return The state machine registry.
     */
    public RaftServiceFactoryRegistry unregister(String type) {
        stateMachines.remove(type);
        return this;
    }

    /**
     * Returns the factory for the given state machine type.
     * @param type The state machine type for which to return the factory.
     * @return The factory for the given state machine type or {@code null} if the type is not registered.
     */
    public Supplier<RaftService> getFactory(String type) {
        return stateMachines.get(type);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("stateMachines", stateMachines)
            .toString();
    }

}

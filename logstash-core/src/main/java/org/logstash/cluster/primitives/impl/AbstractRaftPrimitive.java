package org.logstash.cluster.primitives.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.logstash.cluster.primitives.AsyncPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;

/**
 * Abstract base class for primitives that interact with Raft replicated state machines via proxy.
 */
public abstract class AbstractRaftPrimitive implements AsyncPrimitive {
    protected final RaftProxy proxy;
    private final Function<RaftProxy.State, DistributedPrimitive.Status> mapper = state -> {
        switch (state) {
            case CONNECTED:
                return DistributedPrimitive.Status.ACTIVE;
            case SUSPENDED:
                return DistributedPrimitive.Status.SUSPENDED;
            case CLOSED:
                return DistributedPrimitive.Status.INACTIVE;
            default:
                throw new IllegalStateException("Unknown state " + state);
        }
    };
    private final Set<Consumer<DistributedPrimitive.Status>> statusChangeListeners = Sets.newCopyOnWriteArraySet();

    public AbstractRaftPrimitive(RaftProxy proxy) {
        this.proxy = Preconditions.checkNotNull(proxy, "proxy cannot be null");
        proxy.addStateChangeListener(this::onStateChange);
    }

    @Override
    public String name() {
        return proxy.name();
    }

    @Override
    public void addStatusChangeListener(Consumer<DistributedPrimitive.Status> listener) {
        statusChangeListeners.add(listener);
    }

    @Override
    public void removeStatusChangeListener(Consumer<DistributedPrimitive.Status> listener) {
        statusChangeListeners.remove(listener);
    }

    @Override
    public Collection<Consumer<DistributedPrimitive.Status>> statusChangeListeners() {
        return ImmutableSet.copyOf(statusChangeListeners);
    }

    /**
     * Handles a Raft session state change.
     * @param state the updated Raft session state
     */
    private void onStateChange(RaftProxy.State state) {
        statusChangeListeners.forEach(listener -> listener.accept(mapper.apply(state)));
    }

    @Override
    public CompletableFuture<Void> close() {
        return proxy.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("proxy", proxy)
            .toString();
    }
}

package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxyClient;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * Raft proxy delegate that completes futures on a thread pool.
 */
public class BlockingAwareRaftProxyClient extends DelegatingRaftProxyClient {
    private final Executor executor;
    private final Map<Consumer<RaftProxy.State>, Consumer<RaftProxy.State>> stateChangeListeners = Maps.newConcurrentMap();
    private final Map<Consumer<RaftEvent>, Consumer<RaftEvent>> eventListeners = Maps.newConcurrentMap();

    public BlockingAwareRaftProxyClient(RaftProxyClient delegate, Executor executor) {
        super(delegate);
        this.executor = Preconditions.checkNotNull(executor, "executor cannot be null");
    }

    @Override
    public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
        Consumer<RaftProxy.State> wrappedListener = state -> executor.execute(() -> listener.accept(state));
        stateChangeListeners.put(listener, wrappedListener);
        super.addStateChangeListener(wrappedListener);
    }

    @Override
    public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
        Consumer<RaftProxy.State> wrappedListener = stateChangeListeners.remove(listener);
        if (wrappedListener != null) {
            super.removeStateChangeListener(wrappedListener);
        }
    }

    @Override
    public CompletableFuture<byte[]> execute(RaftOperation operation) {
        return Futures.blockingAwareFuture(super.execute(operation), executor);
    }

    @Override
    public void addEventListener(Consumer<RaftEvent> listener) {
        Consumer<RaftEvent> wrappedListener = e -> executor.execute(() -> listener.accept(e));
        eventListeners.put(listener, wrappedListener);
        super.addEventListener(wrappedListener);
    }

    @Override
    public void removeEventListener(Consumer<RaftEvent> listener) {
        Consumer<RaftEvent> wrappedListener = eventListeners.remove(listener);
        if (wrappedListener != null) {
            super.removeEventListener(wrappedListener);
        }
    }
}

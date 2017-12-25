package org.logstash.cluster.protocols.raft.proxy.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.logstash.cluster.protocols.raft.event.EventType;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxyClient;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.SessionId;

/**
 * Default Raft proxy.
 */
public class DelegatingRaftProxy implements RaftProxy {
    private final RaftProxyClient client;
    private final Map<EventType, Map<Object, Consumer<RaftEvent>>> eventTypeListeners = Maps.newConcurrentMap();

    public DelegatingRaftProxy(RaftProxyClient client) {
        this.client = client;
    }

    @Override
    public SessionId sessionId() {
        return client.sessionId();
    }

    @Override
    public String name() {
        return client.name();
    }

    @Override
    public ServiceType serviceType() {
        return client.serviceType();
    }

    @Override
    public State getState() {
        return client.getState();
    }

    @Override
    public void addStateChangeListener(Consumer<State> listener) {
        client.addStateChangeListener(listener);
    }

    @Override
    public void removeStateChangeListener(Consumer<State> listener) {
        client.removeStateChangeListener(listener);
    }

    @Override
    public CompletableFuture<byte[]> execute(RaftOperation operation) {
        return client.execute(operation);
    }

    @Override
    public void addEventListener(Consumer<RaftEvent> listener) {
        client.addEventListener(listener);
    }

    @Override
    public void removeEventListener(Consumer<RaftEvent> listener) {
        client.removeEventListener(listener);
    }

    @Override
    public <T> void addEventListener(EventType eventType, Function<byte[], T> decoder, Consumer<T> listener) {
        Consumer<RaftEvent> wrappedListener = e -> {
            if (e.type().equals(eventType)) {
                listener.accept(decoder.apply(e.value()));
            }
        };
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
        addEventListener(wrappedListener);
    }

    @Override
    public void addEventListener(EventType eventType, Runnable listener) {
        Consumer<RaftEvent> wrappedListener = e -> {
            if (e.type().equals(eventType)) {
                listener.run();
            }
        };
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
        addEventListener(wrappedListener);
    }

    @Override
    public void addEventListener(EventType eventType, Consumer<byte[]> listener) {
        Consumer<RaftEvent> wrappedListener = e -> {
            if (e.type().equals(eventType)) {
                listener.accept(e.value());
            }
        };
        eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap()).put(listener, wrappedListener);
        addEventListener(wrappedListener);
    }

    @Override
    public void removeEventListener(EventType eventType, Runnable listener) {
        Consumer<RaftEvent> eventListener =
            eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
                .remove(listener);
        removeEventListener(eventListener);
    }

    @Override
    public void removeEventListener(EventType eventType, Consumer listener) {
        Consumer<RaftEvent> eventListener =
            eventTypeListeners.computeIfAbsent(eventType, e -> Maps.newConcurrentMap())
                .remove(listener);
        removeEventListener(eventListener);
    }

    @Override
    public CompletableFuture<RaftProxy> open() {
        return client.open().thenApply(c -> this);
    }

    @Override
    public boolean isOpen() {
        return client.isOpen();
    }

    @Override
    public CompletableFuture<Void> close() {
        return client.close();
    }

    @Override
    public boolean isClosed() {
        return client.isClosed();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("client", client)
            .toString();
    }
}

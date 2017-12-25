package org.logstash.cluster.primitives.multimap.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.multimap.AsyncConsistentMultimap;
import org.logstash.cluster.primitives.multimap.MultimapEvent;
import org.logstash.cluster.primitives.multimap.MultimapEventListener;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;

/**
 * Set based implementation of the {@link AsyncConsistentMultimap}.
 * <p>
 * Note: this implementation does not allow null entries or duplicate entries.
 */
public class RaftConsistentSetMultimap extends AbstractRaftPrimitive
    implements AsyncConsistentMultimap<String, byte[]> {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentSetMultimapOperations.NAMESPACE)
        .register(RaftConsistentSetMultimapEvents.NAMESPACE)
        .build());

    private final Map<MultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

    public RaftConsistentSetMultimap(final RaftProxy proxy) {
        super(proxy);
        proxy.addEventListener(RaftConsistentSetMultimapEvents.CHANGE, SERIALIZER::decode, this::handleEvent);
        proxy.addStateChangeListener(state -> {
            if (state == RaftProxy.State.CONNECTED && isListening()) {
                proxy.invoke(RaftConsistentSetMultimapOperations.ADD_LISTENER);
            }
        });
    }

    private boolean isListening() {
        return !mapEventListeners.isEmpty();
    }

    private void handleEvent(final List<MultimapEvent<String, byte[]>> events) {
        events.forEach(event ->
            mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event))));
    }

    @Override
    public CompletableFuture<Void> clear() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CLEAR);
    }

    @Override
    public CompletableFuture<Integer> size() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.SIZE, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.IS_EMPTY, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsKey(final String key) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CONTAINS_KEY, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.ContainsKey(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(final byte[] value) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CONTAINS_VALUE, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.ContainsValue(value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsEntry(final String key, final byte[] value) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CONTAINS_ENTRY, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.ContainsEntry(key, value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> put(final String key, final byte[] value) {
        return proxy.invoke(
            RaftConsistentSetMultimapOperations.PUT,
            SERIALIZER::encode,
            new RaftConsistentSetMultimapOperations.Put(key, Lists.newArrayList(value), null),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> remove(final String key, final byte[] value) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.REMOVE, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.MultiRemove(key,
            Lists.newArrayList(value),
            null), SERIALIZER::decode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> removeAll(final String key, final Collection<? extends byte[]> values) {
        return proxy.invoke(
            RaftConsistentSetMultimapOperations.REMOVE,
            SERIALIZER::encode,
            new RaftConsistentSetMultimapOperations.MultiRemove(key, (Collection<byte[]>) values, null),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> removeAll(final String key) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.REMOVE_ALL, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.RemoveAll(key, null), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> putAll(
        final String key, final Collection<? extends byte[]> values) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.PUT, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.Put(key, values, null), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> replaceValues(
        final String key, final Collection<byte[]> values) {
        return proxy.invoke(
            RaftConsistentSetMultimapOperations.REPLACE,
            SERIALIZER::encode,
            new RaftConsistentSetMultimapOperations.Replace(key, values, null),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> get(final String key) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.GET, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.Get(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Set<String>> keySet() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.KEY_SET, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Multiset<String>> keys() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.KEYS, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Multiset<byte[]>> values() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.VALUES, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Collection<Map.Entry<String, byte[]>>> entries() {
        return proxy.invoke(RaftConsistentSetMultimapOperations.ENTRIES, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> addListener(final MultimapEventListener<String, byte[]> listener, final Executor executor) {
        if (mapEventListeners.isEmpty()) {
            return proxy.invoke(RaftConsistentSetMultimapOperations.ADD_LISTENER).thenRun(() -> mapEventListeners.put(listener, executor));
        } else {
            mapEventListeners.put(listener, executor);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(final MultimapEventListener<String, byte[]> listener) {
        if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
            return proxy.invoke(RaftConsistentSetMultimapOperations.REMOVE_LISTENER).thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Map<String, Collection<byte[]>>> asMap() {
        throw new UnsupportedOperationException("Expensive operation.");
    }
}

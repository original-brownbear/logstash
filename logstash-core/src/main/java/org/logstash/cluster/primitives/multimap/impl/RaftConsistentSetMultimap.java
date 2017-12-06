/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public class RaftConsistentSetMultimap
    extends AbstractRaftPrimitive
    implements AsyncConsistentMultimap<String, byte[]> {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentSetMultimapOperations.NAMESPACE)
        .register(RaftConsistentSetMultimapEvents.NAMESPACE)
        .build());

    private final Map<MultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

    public RaftConsistentSetMultimap(RaftProxy proxy) {
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

    private void handleEvent(List<MultimapEvent<String, byte[]>> events) {
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
    public CompletableFuture<Boolean> containsKey(String key) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CONTAINS_KEY, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.ContainsKey(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(byte[] value) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CONTAINS_VALUE, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.ContainsValue(value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsEntry(String key, byte[] value) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.CONTAINS_ENTRY, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.ContainsEntry(key, value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> put(String key, byte[] value) {
        return proxy.invoke(
            RaftConsistentSetMultimapOperations.PUT,
            SERIALIZER::encode,
            new RaftConsistentSetMultimapOperations.Put(key, Lists.newArrayList(value), null),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> remove(String key, byte[] value) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.REMOVE, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.MultiRemove(key,
            Lists.newArrayList(value),
            null), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> removeAll(String key, Collection<? extends byte[]> values) {
        return proxy.invoke(
            RaftConsistentSetMultimapOperations.REMOVE,
            SERIALIZER::encode,
            new RaftConsistentSetMultimapOperations.MultiRemove(key, (Collection<byte[]>) values, null),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> removeAll(String key) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.REMOVE_ALL, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.RemoveAll(key, null), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> putAll(
        String key, Collection<? extends byte[]> values) {
        return proxy.invoke(RaftConsistentSetMultimapOperations.PUT, SERIALIZER::encode, new RaftConsistentSetMultimapOperations.Put(key, values, null), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> replaceValues(
        String key, Collection<byte[]> values) {
        return proxy.invoke(
            RaftConsistentSetMultimapOperations.REPLACE,
            SERIALIZER::encode,
            new RaftConsistentSetMultimapOperations.Replace(key, values, null),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Versioned<Collection<? extends byte[]>>> get(String key) {
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
    public CompletableFuture<Void> addListener(MultimapEventListener<String, byte[]> listener, Executor executor) {
        if (mapEventListeners.isEmpty()) {
            return proxy.invoke(RaftConsistentSetMultimapOperations.ADD_LISTENER).thenRun(() -> mapEventListeners.put(listener, executor));
        } else {
            mapEventListeners.put(listener, executor);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(MultimapEventListener<String, byte[]> listener) {
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

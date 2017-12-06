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
package org.logstash.cluster.primitives.map.impl;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.primitives.map.ConsistentMapException;
import org.logstash.cluster.primitives.map.MapEvent;
import org.logstash.cluster.primitives.map.MapEventListener;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Version;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * Distributed resource providing the {@link AsyncConsistentMap} primitive.
 */
public class RaftConsistentMap extends AbstractRaftPrimitive implements AsyncConsistentMap<String, byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentMapOperations.NAMESPACE)
        .register(RaftConsistentMapEvents.NAMESPACE)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
        .build());

    private final Map<MapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

    public RaftConsistentMap(RaftProxy proxy) {
        super(proxy);
        proxy.addEventListener(RaftConsistentMapEvents.CHANGE, SERIALIZER::decode, this::handleEvent);
        proxy.addStateChangeListener(state -> {
            if (state == RaftProxy.State.CONNECTED && isListening()) {
                proxy.invoke(RaftConsistentMapOperations.ADD_LISTENER);
            }
        });
    }

    private boolean isListening() {
        return !mapEventListeners.isEmpty();
    }

    private void handleEvent(List<MapEvent<String, byte[]>> events) {
        events.forEach(event ->
            mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event))));
    }

    @Override
    public CompletableFuture<Void> clear() {
        return proxy.<MapEntryUpdateResult.Status>invoke(RaftConsistentMapOperations.CLEAR, serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
        return proxy.invoke(RaftConsistentMapOperations.IS_EMPTY, serializer()::decode);
    }

    protected Serializer serializer() {
        return SERIALIZER;
    }

    @Override
    public CompletableFuture<Integer> size() {
        return proxy.invoke(RaftConsistentMapOperations.SIZE, serializer()::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsKey(String key) {
        return proxy.invoke(RaftConsistentMapOperations.CONTAINS_KEY, serializer()::encode, new RaftConsistentMapOperations.ContainsKey(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(byte[] value) {
        return proxy.invoke(RaftConsistentMapOperations.CONTAINS_VALUE, serializer()::encode, new RaftConsistentMapOperations.ContainsValue(value), serializer()::decode);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> get(String key) {
        return proxy.invoke(RaftConsistentMapOperations.GET, serializer()::encode, new RaftConsistentMapOperations.Get(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<Map<String, Versioned<byte[]>>> getAllPresent(Iterable<String> keys) {
        Set<String> uniqueKeys = new HashSet<>();
        for (String key : keys) {
            uniqueKeys.add(key);
        }
        return proxy.invoke(
            RaftConsistentMapOperations.GET_ALL_PRESENT,
            serializer()::encode,
            new RaftConsistentMapOperations.GetAllPresent(uniqueKeys),
            serializer()::decode);
    }

    @Override
    public CompletableFuture<Versioned<byte[]>> getOrDefault(String key, byte[] defaultValue) {
        return proxy.invoke(
            RaftConsistentMapOperations.GET_OR_DEFAULT,
            serializer()::encode,
            new RaftConsistentMapOperations.GetOrDefault(key, defaultValue),
            serializer()::decode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<byte[]>> computeIf(String key,
        Predicate<? super byte[]> condition,
        BiFunction<? super String, ? super byte[], ? extends byte[]> remappingFunction) {
        return get(key).thenCompose(r1 -> {
            byte[] existingValue = r1 == null ? null : r1.value();
            // if the condition evaluates to false, return existing value.
            if (!condition.test(existingValue)) {
                return CompletableFuture.completedFuture(r1);
            }

            byte[] computedValue;
            try {
                computedValue = remappingFunction.apply(key, existingValue);
            } catch (Exception e) {
                return Futures.exceptionalFuture(e);
            }

            if (computedValue == null && r1 == null) {
                return CompletableFuture.completedFuture(null);
            }

            if (r1 == null) {
                return proxy.<RaftConsistentMapOperations.Put, MapEntryUpdateResult<String, byte[]>>invoke(
                    RaftConsistentMapOperations.PUT_IF_ABSENT,
                    serializer()::encode,
                    new RaftConsistentMapOperations.Put(key, computedValue),
                    serializer()::decode)
                    .whenComplete((r, e) -> throwIfLocked(r))
                    .thenCompose(r -> checkLocked(r))
                    .thenApply(result -> new Versioned<>(computedValue, result.version()));
            } else if (computedValue == null) {
                return proxy.<RaftConsistentMapOperations.RemoveVersion, MapEntryUpdateResult<String, byte[]>>invoke(
                    RaftConsistentMapOperations.REMOVE_VERSION,
                    serializer()::encode,
                    new RaftConsistentMapOperations.RemoveVersion(key, r1.version()),
                    serializer()::decode)
                    .whenComplete((r, e) -> throwIfLocked(r))
                    .thenCompose(r -> checkLocked(r))
                    .thenApply(v -> null);
            } else {
                return proxy.<RaftConsistentMapOperations.ReplaceVersion, MapEntryUpdateResult<String, byte[]>>invoke(
                    RaftConsistentMapOperations.REPLACE_VERSION,
                    serializer()::encode,
                    new RaftConsistentMapOperations.ReplaceVersion(key, r1.version(), computedValue),
                    serializer()::decode)
                    .whenComplete((r, e) -> throwIfLocked(r))
                    .thenCompose(r -> checkLocked(r))
                    .thenApply(result -> result.status() == MapEntryUpdateResult.Status.OK
                        ? new Versioned(computedValue, result.version()) : result.result());
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value) {
        return proxy.<RaftConsistentMapOperations.Put, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.PUT,
            serializer()::encode,
            new RaftConsistentMapOperations.Put(key, value),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.result());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<byte[]>> putAndGet(String key, byte[] value) {
        return proxy.<RaftConsistentMapOperations.Put, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.PUT_AND_GET,
            serializer()::encode,
            new RaftConsistentMapOperations.Put(key, value),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.result());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<byte[]>> remove(String key) {
        return proxy.<RaftConsistentMapOperations.Remove, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.REMOVE,
            serializer()::encode,
            new RaftConsistentMapOperations.Remove(key),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.result());
    }

    @Override
    public CompletableFuture<Set<String>> keySet() {
        return proxy.invoke(RaftConsistentMapOperations.KEY_SET, serializer()::decode);
    }

    @Override
    public CompletableFuture<Collection<Versioned<byte[]>>> values() {
        return proxy.invoke(RaftConsistentMapOperations.VALUES, serializer()::decode);
    }

    @Override
    public CompletableFuture<Set<Entry<String, Versioned<byte[]>>>> entrySet() {
        return proxy.invoke(RaftConsistentMapOperations.ENTRY_SET, serializer()::decode);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value) {
        return proxy.<RaftConsistentMapOperations.Put, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.PUT_IF_ABSENT,
            serializer()::encode,
            new RaftConsistentMapOperations.Put(key, value),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.result());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> remove(String key, byte[] value) {
        return proxy.<RaftConsistentMapOperations.RemoveValue, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.REMOVE_VALUE,
            serializer()::encode,
            new RaftConsistentMapOperations.RemoveValue(key, value),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.updated());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> remove(String key, long version) {
        return proxy.<RaftConsistentMapOperations.RemoveVersion, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.REMOVE_VERSION,
            serializer()::encode,
            new RaftConsistentMapOperations.RemoveVersion(key, version),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.updated());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
        return proxy.<RaftConsistentMapOperations.Replace, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.REPLACE,
            serializer()::encode,
            new RaftConsistentMapOperations.Replace(key, value),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.result());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
        return proxy.<RaftConsistentMapOperations.ReplaceValue, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.REPLACE_VALUE,
            serializer()::encode,
            new RaftConsistentMapOperations.ReplaceValue(key, oldValue, newValue),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.updated());
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
        return proxy.<RaftConsistentMapOperations.ReplaceVersion, MapEntryUpdateResult<String, byte[]>>invoke(
            RaftConsistentMapOperations.REPLACE_VERSION,
            serializer()::encode,
            new RaftConsistentMapOperations.ReplaceVersion(key, oldVersion, newValue),
            serializer()::decode)
            .whenComplete((r, e) -> throwIfLocked(r))
            .thenApply(v -> v.updated());
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(MapEventListener<String, byte[]> listener,
        Executor executor) {
        if (mapEventListeners.isEmpty()) {
            return proxy.invoke(RaftConsistentMapOperations.ADD_LISTENER).thenRun(() -> mapEventListeners.put(listener, executor));
        } else {
            mapEventListeners.put(listener, executor);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(MapEventListener<String, byte[]> listener) {
        if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
            return proxy.invoke(RaftConsistentMapOperations.REMOVE_LISTENER).thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }

    private void throwIfLocked(MapEntryUpdateResult<String, byte[]> result) {
        if (result != null) {
            throwIfLocked(result.status());
        }
    }

    private void throwIfLocked(MapEntryUpdateResult.Status status) {
        if (status == MapEntryUpdateResult.Status.WRITE_LOCK) {
            throw new ConcurrentModificationException("Cannot update map: Another transaction in progress");
        }
    }

    private CompletableFuture<MapEntryUpdateResult<String, byte[]>> checkLocked(
        MapEntryUpdateResult<String, byte[]> result) {
        if (result.status() == MapEntryUpdateResult.Status.PRECONDITION_FAILED ||
            result.status() == MapEntryUpdateResult.Status.WRITE_LOCK) {
            return Futures.exceptionalFuture(new ConsistentMapException.ConcurrentModification());
        }
        return CompletableFuture.completedFuture(result);
    }

    @Override
    public CompletableFuture<Version> begin(TransactionId transactionId) {
        return proxy.<RaftConsistentMapOperations.TransactionBegin, Long>invoke(
            RaftConsistentMapOperations.BEGIN,
            serializer()::encode,
            new RaftConsistentMapOperations.TransactionBegin(transactionId),
            serializer()::decode)
            .thenApply(Version::new);
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
        return proxy.<RaftConsistentMapOperations.TransactionPrepare, PrepareResult>invoke(
            RaftConsistentMapOperations.PREPARE,
            serializer()::encode,
            new RaftConsistentMapOperations.TransactionPrepare(transactionLog),
            serializer()::decode)
            .thenApply(v -> v == PrepareResult.OK);
    }

    @Override
    public CompletableFuture<Boolean> prepareAndCommit(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
        return proxy.<RaftConsistentMapOperations.TransactionPrepareAndCommit, PrepareResult>invoke(
            RaftConsistentMapOperations.PREPARE_AND_COMMIT,
            serializer()::encode,
            new RaftConsistentMapOperations.TransactionPrepareAndCommit(transactionLog),
            serializer()::decode)
            .thenApply(v -> v == PrepareResult.OK);
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
        return proxy.<RaftConsistentMapOperations.TransactionCommit, CommitResult>invoke(
            RaftConsistentMapOperations.COMMIT,
            serializer()::encode,
            new RaftConsistentMapOperations.TransactionCommit(transactionId),
            serializer()::decode)
            .thenApply(v -> null);
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
        return proxy.invoke(
            RaftConsistentMapOperations.ROLLBACK,
            serializer()::encode,
            new RaftConsistentMapOperations.TransactionRollback(transactionId),
            serializer()::decode)
            .thenApply(v -> null);
    }
}

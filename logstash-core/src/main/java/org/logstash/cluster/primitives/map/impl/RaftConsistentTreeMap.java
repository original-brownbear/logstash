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

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.primitives.map.AsyncConsistentTreeMap;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;

/**
 * Implementation of {@link io.atomix.primitives.map.AsyncConsistentTreeMap}.
 */
public class RaftConsistentTreeMap extends RaftConsistentMap implements AsyncConsistentTreeMap<String, byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftConsistentMapOperations.NAMESPACE)
        .register(RaftConsistentTreeMapOperations.NAMESPACE)
        .register(RaftConsistentMapEvents.NAMESPACE)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 150)
        .register(RaftConsistentMapService.TransactionScope.class)
        .register(TransactionLog.class)
        .register(TransactionId.class)
        .register(RaftConsistentMapService.MapEntryValue.class)
        .register(RaftConsistentMapService.MapEntryValue.Type.class)
        .register(new HashMap().keySet().getClass())
        .register(TreeMap.class)
        .build());

    public RaftConsistentTreeMap(RaftProxy proxy) {
        super(proxy);
    }

    @Override
    public CompletableFuture<String> firstKey() {
        return proxy.invoke(RaftConsistentTreeMapOperations.FIRST_KEY, serializer()::decode);
    }

    @Override
    protected Serializer serializer() {
        return SERIALIZER;
    }

    @Override
    public CompletableFuture<String> lastKey() {
        return proxy.invoke(RaftConsistentTreeMapOperations.LAST_KEY, serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> ceilingEntry(String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.CEILING_ENTRY, serializer()::encode, new RaftConsistentTreeMapOperations.CeilingEntry(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> floorEntry(String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.FLOOR_ENTRY, serializer()::encode, new RaftConsistentTreeMapOperations.FloorEntry(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> higherEntry(
        String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.HIGHER_ENTRY, serializer()::encode, new RaftConsistentTreeMapOperations.HigherEntry(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lowerEntry(
        String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.LOWER_ENTRY, serializer()::encode, new RaftConsistentTreeMapOperations.LowerEntry(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> firstEntry() {
        return proxy.invoke(RaftConsistentTreeMapOperations.FIRST_ENTRY, serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> lastEntry() {
        return proxy.invoke(RaftConsistentTreeMapOperations.LAST_ENTRY, serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> pollFirstEntry() {
        return proxy.invoke(RaftConsistentTreeMapOperations.POLL_FIRST_ENTRY, serializer()::decode);
    }

    @Override
    public CompletableFuture<Map.Entry<String, Versioned<byte[]>>> pollLastEntry() {
        return proxy.invoke(RaftConsistentTreeMapOperations.POLL_LAST_ENTRY, serializer()::decode);
    }

    @Override
    public CompletableFuture<String> lowerKey(String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.LOWER_KEY, serializer()::encode, new RaftConsistentTreeMapOperations.LowerKey(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<String> floorKey(String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.FLOOR_KEY, serializer()::encode, new RaftConsistentTreeMapOperations.FloorKey(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<String> ceilingKey(String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.CEILING_KEY, serializer()::encode, new RaftConsistentTreeMapOperations.CeilingKey(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<String> higherKey(String key) {
        return proxy.invoke(RaftConsistentTreeMapOperations.HIGHER_KEY, serializer()::encode, new RaftConsistentTreeMapOperations.HigherKey(key), serializer()::decode);
    }

    @Override
    public CompletableFuture<NavigableSet<String>> navigableKeySet() {
        throw new UnsupportedOperationException("This operation is not yet supported.");
    }

    @Override
    public CompletableFuture<NavigableMap<String, byte[]>> subMap(
        String upperKey, String lowerKey, boolean inclusiveUpper,
        boolean inclusiveLower) {
        throw new UnsupportedOperationException("This operation is not yet supported.");
    }
}

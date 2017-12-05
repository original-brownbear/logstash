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
package org.logstash.cluster.primitives.value.impl;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.primitives.value.AtomicValueEventListener;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Atomix counter implementation.
 */
public class RaftAtomicValue extends AbstractRaftPrimitive implements AsyncAtomicValue<byte[]> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftAtomicValueOperations.NAMESPACE)
        .register(RaftAtomicValueEvents.NAMESPACE)
        .build());

    private final Set<AtomicValueEventListener<byte[]>> eventListeners = Sets.newConcurrentHashSet();

    public RaftAtomicValue(RaftProxy proxy) {
        super(proxy);
    }

    @Override
    public CompletableFuture<byte[]> get() {
        return proxy.invoke(RaftAtomicValueOperations.GET, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> set(byte[] value) {
        return proxy.invoke(RaftAtomicValueOperations.SET, SERIALIZER::encode, new RaftAtomicValueOperations.Set(value));
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(byte[] expect, byte[] update) {
        return proxy.invoke(RaftAtomicValueOperations.COMPARE_AND_SET, SERIALIZER::encode,
            new RaftAtomicValueOperations.CompareAndSet(expect, update), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<byte[]> getAndSet(byte[] value) {
        return proxy.invoke(RaftAtomicValueOperations.GET_AND_SET, SERIALIZER::encode, new RaftAtomicValueOperations.GetAndSet(value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> addListener(AtomicValueEventListener<byte[]> listener) {
        if (eventListeners.isEmpty()) {
            return proxy.invoke(RaftAtomicValueOperations.ADD_LISTENER).thenRun(() -> eventListeners.add(listener));
        } else {
            eventListeners.add(listener);
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(AtomicValueEventListener<byte[]> listener) {
        if (eventListeners.remove(listener) && eventListeners.isEmpty()) {
            return proxy.invoke(RaftAtomicValueOperations.REMOVE_LISTENER).thenApply(v -> null);
        }
        return CompletableFuture.completedFuture(null);
    }
}

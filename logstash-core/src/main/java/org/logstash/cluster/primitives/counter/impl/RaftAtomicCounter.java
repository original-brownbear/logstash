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
package org.logstash.cluster.primitives.counter.impl;

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.counter.AsyncAtomicCounter;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Atomix counter implementation.
 */
public class RaftAtomicCounter extends AbstractRaftPrimitive implements AsyncAtomicCounter {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftAtomicCounterOperations.NAMESPACE)
        .build());

    public RaftAtomicCounter(RaftProxy proxy) {
        super(proxy);
    }

    private static long nullOrZero(Long value) {
        return value != null ? value : 0;
    }

    @Override
    public CompletableFuture<Long> incrementAndGet() {
        return proxy.invoke(RaftAtomicCounterOperations.INCREMENT_AND_GET, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> getAndIncrement() {
        return proxy.invoke(RaftAtomicCounterOperations.GET_AND_INCREMENT, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> getAndAdd(long delta) {
        return proxy.invoke(RaftAtomicCounterOperations.GET_AND_ADD, SERIALIZER::encode, new RaftAtomicCounterOperations.GetAndAdd(delta), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> addAndGet(long delta) {
        return proxy.invoke(RaftAtomicCounterOperations.ADD_AND_GET, SERIALIZER::encode, new RaftAtomicCounterOperations.AddAndGet(delta), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> get() {
        return proxy.<Long>invoke(RaftAtomicCounterOperations.GET, SERIALIZER::decode).thenApply(RaftAtomicCounter::nullOrZero);
    }

    @Override
    public CompletableFuture<Void> set(long value) {
        return proxy.invoke(RaftAtomicCounterOperations.SET, SERIALIZER::encode, new RaftAtomicCounterOperations.Set(value));
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
        return proxy.invoke(RaftAtomicCounterOperations.COMPARE_AND_SET, SERIALIZER::encode,
            new RaftAtomicCounterOperations.CompareAndSet(expectedValue, updateValue), SERIALIZER::decode);
    }
}

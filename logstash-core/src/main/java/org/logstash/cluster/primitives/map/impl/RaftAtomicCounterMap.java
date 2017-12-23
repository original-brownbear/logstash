package org.logstash.cluster.primitives.map.impl;

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.impl.AbstractRaftPrimitive;
import org.logstash.cluster.primitives.map.AsyncAtomicCounterMap;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * {@code AsyncAtomicCounterMap} implementation backed by Atomix.
 */
public class RaftAtomicCounterMap extends AbstractRaftPrimitive implements AsyncAtomicCounterMap<String> {
    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftAtomicCounterMapOperations.NAMESPACE)
        .build());

    public RaftAtomicCounterMap(RaftProxy proxy) {
        super(proxy);
    }

    @Override
    public CompletableFuture<Long> incrementAndGet(String key) {
        return proxy.invoke(RaftAtomicCounterMapOperations.INCREMENT_AND_GET, SERIALIZER::encode, new RaftAtomicCounterMapOperations.IncrementAndGet(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> decrementAndGet(String key) {
        return proxy.invoke(RaftAtomicCounterMapOperations.DECREMENT_AND_GET, SERIALIZER::encode, new RaftAtomicCounterMapOperations.DecrementAndGet(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> getAndIncrement(String key) {
        return proxy.invoke(RaftAtomicCounterMapOperations.GET_AND_INCREMENT, SERIALIZER::encode, new RaftAtomicCounterMapOperations.GetAndIncrement(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> getAndDecrement(String key) {
        return proxy.invoke(RaftAtomicCounterMapOperations.GET_AND_DECREMENT, SERIALIZER::encode, new RaftAtomicCounterMapOperations.GetAndDecrement(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> addAndGet(String key, long delta) {
        return proxy.invoke(RaftAtomicCounterMapOperations.ADD_AND_GET, SERIALIZER::encode, new RaftAtomicCounterMapOperations.AddAndGet(key, delta), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> getAndAdd(String key, long delta) {
        return proxy.invoke(RaftAtomicCounterMapOperations.GET_AND_ADD, SERIALIZER::encode, new RaftAtomicCounterMapOperations.GetAndAdd(key, delta), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> get(String key) {
        return proxy.invoke(RaftAtomicCounterMapOperations.GET, SERIALIZER::encode, new RaftAtomicCounterMapOperations.Get(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> put(String key, long newValue) {
        return proxy.invoke(RaftAtomicCounterMapOperations.PUT, SERIALIZER::encode, new RaftAtomicCounterMapOperations.Put(key, newValue), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> putIfAbsent(String key, long newValue) {
        return proxy.invoke(RaftAtomicCounterMapOperations.PUT_IF_ABSENT, SERIALIZER::encode, new RaftAtomicCounterMapOperations.PutIfAbsent(key, newValue), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> replace(String key, long expectedOldValue, long newValue) {
        return proxy.invoke(
            RaftAtomicCounterMapOperations.REPLACE,
            SERIALIZER::encode,
            new RaftAtomicCounterMapOperations.Replace(key, expectedOldValue, newValue),
            SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Long> remove(String key) {
        return proxy.invoke(RaftAtomicCounterMapOperations.REMOVE, SERIALIZER::encode, new RaftAtomicCounterMapOperations.Remove(key), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> remove(String key, long value) {
        return proxy.invoke(RaftAtomicCounterMapOperations.REMOVE_VALUE, SERIALIZER::encode, new RaftAtomicCounterMapOperations.RemoveValue(key, value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Integer> size() {
        return proxy.invoke(RaftAtomicCounterMapOperations.SIZE, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
        return proxy.invoke(RaftAtomicCounterMapOperations.IS_EMPTY, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> clear() {
        return proxy.invoke(RaftAtomicCounterMapOperations.CLEAR);
    }
}

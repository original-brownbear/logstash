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
    public CompletableFuture<Boolean> compareAndSet(byte[] expect, byte[] update) {
        return proxy.invoke(RaftAtomicValueOperations.COMPARE_AND_SET, SERIALIZER::encode,
            new RaftAtomicValueOperations.CompareAndSet(expect, update), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<byte[]> get() {
        return proxy.invoke(RaftAtomicValueOperations.GET, SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<byte[]> getAndSet(byte[] value) {
        return proxy.invoke(RaftAtomicValueOperations.GET_AND_SET, SERIALIZER::encode, new RaftAtomicValueOperations.GetAndSet(value), SERIALIZER::decode);
    }

    @Override
    public CompletableFuture<Void> set(byte[] value) {
        return proxy.invoke(RaftAtomicValueOperations.SET, SERIALIZER::encode, new RaftAtomicValueOperations.Set(value));
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

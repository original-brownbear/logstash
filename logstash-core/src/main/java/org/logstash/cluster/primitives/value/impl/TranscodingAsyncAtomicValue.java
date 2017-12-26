package org.logstash.cluster.primitives.value.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.primitives.value.AtomicValueEvent;
import org.logstash.cluster.primitives.value.AtomicValueEventListener;

/**
 * Transcoding async atomic value.
 */
public class TranscodingAsyncAtomicValue<V1, V2> implements AsyncAtomicValue<V1> {

    private final AsyncAtomicValue<V2> backingValue;
    private final Function<V1, V2> valueEncoder;
    private final Function<V2, V1> valueDecoder;
    private final Map<AtomicValueEventListener<V1>, TranscodingAsyncAtomicValue.InternalAtomicValueEventListener> listeners = Maps.newIdentityHashMap();

    public TranscodingAsyncAtomicValue(AsyncAtomicValue<V2> backingValue, Function<V1, V2> valueEncoder, Function<V2, V1> valueDecoder) {
        this.backingValue = backingValue;
        this.valueEncoder = valueEncoder;
        this.valueDecoder = valueDecoder;
    }

    @Override
    public String name() {
        return backingValue.name();
    }

    @Override
    public CompletableFuture<Boolean> compareAndSet(V1 expect, V1 update) {
        return backingValue.compareAndSet(valueEncoder.apply(expect), valueEncoder.apply(update));
    }

    @Override
    public CompletableFuture<V1> get() {
        return backingValue.get().thenApply(valueDecoder);
    }

    @Override
    public CompletableFuture<V1> getAndSet(V1 value) {
        return backingValue.getAndSet(valueEncoder.apply(value)).thenApply(valueDecoder);
    }

    @Override
    public CompletableFuture<Void> set(V1 value) {
        return backingValue.set(valueEncoder.apply(value));
    }

    @Override
    public CompletableFuture<Void> addListener(AtomicValueEventListener<V1> listener) {
        synchronized (listeners) {
            TranscodingAsyncAtomicValue.InternalAtomicValueEventListener internalListener =
                listeners.computeIfAbsent(listener, k -> new TranscodingAsyncAtomicValue.InternalAtomicValueEventListener(listener));
            return backingValue.addListener(internalListener);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(AtomicValueEventListener<V1> listener) {
        synchronized (listeners) {
            TranscodingAsyncAtomicValue.InternalAtomicValueEventListener internalListener = listeners.remove(listener);
            if (internalListener != null) {
                return backingValue.removeListener(internalListener);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return backingValue.close();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("backingValue", backingValue)
            .toString();
    }

    private class InternalAtomicValueEventListener implements AtomicValueEventListener<V2> {
        private final AtomicValueEventListener<V1> listener;

        InternalAtomicValueEventListener(AtomicValueEventListener<V1> listener) {
            this.listener = listener;
        }

        @Override
        public void event(AtomicValueEvent<V2> event) {
            listener.event(new AtomicValueEvent<>(
                valueDecoder.apply(event.newValue()),
                valueDecoder.apply(event.oldValue())));
        }
    }
}

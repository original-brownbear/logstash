package org.logstash.cluster.primitives.map.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.primitives.map.MapEvent;
import org.logstash.cluster.primitives.map.MapEventListener;
import org.logstash.cluster.time.Version;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * An {@code AsyncConsistentMap} that maps its operations to operations on a
 * differently typed {@code AsyncConsistentMap} by transcoding operation inputs and outputs.
 * @param <K2> key type of other map
 * @param <V2> value type of other map
 * @param <K1> key type of this map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncConsistentMap<K1, V1, K2, V2> implements AsyncConsistentMap<K1, V1> {

    private final AsyncConsistentMap<K2, V2> backingMap;
    private final Function<K1, K2> keyEncoder;
    private final Function<K2, K1> keyDecoder;
    private final Function<V2, V1> valueDecoder;
    private final Function<V1, V2> valueEncoder;
    private final Function<Versioned<V2>, Versioned<V1>> versionedValueTransform;
    private final Map<MapEventListener<K1, V1>, TranscodingAsyncConsistentMap.InternalBackingMapEventListener> listeners =
        Maps.newIdentityHashMap();

    public TranscodingAsyncConsistentMap(AsyncConsistentMap<K2, V2> backingMap,
        Function<K1, K2> keyEncoder,
        Function<K2, K1> keyDecoder,
        Function<V1, V2> valueEncoder,
        Function<V2, V1> valueDecoder) {
        this.backingMap = backingMap;
        this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
        this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
        this.valueEncoder = v -> v == null ? null : valueEncoder.apply(v);
        this.valueDecoder = v -> v == null ? null : valueDecoder.apply(v);
        this.versionedValueTransform = v -> v == null ? null : v.map(valueDecoder);
    }

    @Override
    public String name() {
        return backingMap.name();
    }

    @Override
    public void addStatusChangeListener(Consumer<DistributedPrimitive.Status> listener) {
        backingMap.addStatusChangeListener(listener);
    }

    @Override
    public void removeStatusChangeListener(Consumer<DistributedPrimitive.Status> listener) {
        backingMap.removeStatusChangeListener(listener);
    }

    @Override
    public Collection<Consumer<DistributedPrimitive.Status>> statusChangeListeners() {
        return backingMap.statusChangeListeners();
    }

    @Override
    public CompletableFuture<Void> clear() {
        return backingMap.clear();
    }

    @Override
    public CompletableFuture<Integer> size() {
        return backingMap.size();
    }

    @Override
    public CompletableFuture<Boolean> containsKey(K1 key) {
        try {
            return backingMap.containsKey(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> containsValue(V1 value) {
        try {
            return backingMap.containsValue(valueEncoder.apply(value));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> get(K1 key) {
        try {
            return backingMap.get(keyEncoder.apply(key)).thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Map<K1, Versioned<V1>>> getAllPresent(Iterable<K1> keys) {
        try {
            Set<K2> uniqueKeys = new HashSet<>();
            for (K1 key : keys) {
                uniqueKeys.add(keyEncoder.apply(key));
            }
            return backingMap.getAllPresent(uniqueKeys).thenApply(
                entries -> ImmutableMap.copyOf(entries.entrySet().stream()
                    .collect(Collectors.toMap(o -> keyDecoder.apply(o.getKey()),
                        o -> versionedValueTransform.apply(o.getValue())))));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> getOrDefault(K1 key, V1 defaultValue) {
        try {
            return backingMap.getOrDefault(keyEncoder.apply(key), valueEncoder.apply(defaultValue))
                .thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> computeIf(K1 key,
        Predicate<? super V1> condition,
        BiFunction<? super K1, ? super V1, ? extends V1> remappingFunction) {
        try {
            return backingMap.computeIf(keyEncoder.apply(key),
                v -> condition.test(valueDecoder.apply(v)),
                (k, v) -> valueEncoder.apply(remappingFunction.apply(keyDecoder.apply(k),
                    valueDecoder.apply(v))))
                .thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> put(K1 key, V1 value) {
        try {
            return backingMap.put(keyEncoder.apply(key), valueEncoder.apply(value))
                .thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> putAndGet(K1 key, V1 value) {
        try {
            return backingMap.putAndGet(keyEncoder.apply(key), valueEncoder.apply(value))
                .thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> remove(K1 key) {
        try {
            return backingMap.remove(keyEncoder.apply(key)).thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Set<K1>> keySet() {
        return backingMap.keySet()
            .thenApply(s -> s.stream().map(keyDecoder).collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Collection<Versioned<V1>>> values() {
        return backingMap.values()
            .thenApply(c -> c.stream().map(versionedValueTransform).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Set<Map.Entry<K1, Versioned<V1>>>> entrySet() {
        return backingMap.entrySet()
            .thenApply(s -> s.stream()
                .map(e -> Maps.immutableEntry(keyDecoder.apply(e.getKey()),
                    versionedValueTransform.apply(e.getValue())))
                .collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Versioned<V1>> putIfAbsent(K1 key, V1 value) {
        try {
            return backingMap.putIfAbsent(keyEncoder.apply(key), valueEncoder.apply(value))
                .thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> remove(K1 key, V1 value) {
        try {
            return backingMap.remove(keyEncoder.apply(key), valueEncoder.apply(value));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> remove(K1 key, long version) {
        try {
            return backingMap.remove(keyEncoder.apply(key), version);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Versioned<V1>> replace(K1 key, V1 value) {
        try {
            return backingMap.replace(keyEncoder.apply(key), valueEncoder.apply(value))
                .thenApply(versionedValueTransform);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> replace(K1 key, V1 oldValue, V1 newValue) {
        try {
            return backingMap.replace(keyEncoder.apply(key),
                valueEncoder.apply(oldValue),
                valueEncoder.apply(newValue));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> replace(K1 key, long oldVersion, V1 newValue) {
        try {
            return backingMap.replace(keyEncoder.apply(key), oldVersion, valueEncoder.apply(newValue));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> addListener(MapEventListener<K1, V1> listener, Executor executor) {
        synchronized (listeners) {
            TranscodingAsyncConsistentMap.InternalBackingMapEventListener backingMapListener =
                listeners.computeIfAbsent(listener, k -> new TranscodingAsyncConsistentMap.InternalBackingMapEventListener(listener));
            return backingMap.addListener(backingMapListener, executor);
        }
    }

    @Override
    public CompletableFuture<Void> removeListener(MapEventListener<K1, V1> listener) {
        synchronized (listeners) {
            TranscodingAsyncConsistentMap.InternalBackingMapEventListener backingMapListener = listeners.remove(listener);
            if (backingMapListener != null) {
                return backingMap.removeListener(backingMapListener);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    @Override
    public CompletableFuture<Version> begin(TransactionId transactionId) {
        try {
            return backingMap.begin(transactionId);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K1, V1>> transactionLog) {
        try {
            return backingMap.prepare(transactionLog.map(record -> record.map(keyEncoder, valueEncoder)));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> prepareAndCommit(TransactionLog<MapUpdate<K1, V1>> transactionLog) {
        try {
            return backingMap.prepareAndCommit(transactionLog.map(record -> record.map(keyEncoder, valueEncoder)));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId transactionId) {
        try {
            return backingMap.commit(transactionId);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> rollback(TransactionId transactionId) {
        try {
            return backingMap.rollback(transactionId);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return backingMap.close();
    }

    private class InternalBackingMapEventListener implements MapEventListener<K2, V2> {

        private final MapEventListener<K1, V1> listener;

        InternalBackingMapEventListener(MapEventListener<K1, V1> listener) {
            this.listener = listener;
        }

        @Override
        public void event(MapEvent<K2, V2> event) {
            listener.event(new MapEvent<>(
                event.type(),
                event.name(),
                keyDecoder.apply(event.key()),
                event.newValue() != null ? event.newValue().map(valueDecoder) : null,
                event.oldValue() != null ? event.oldValue().map(valueDecoder) : null));
        }
    }
}

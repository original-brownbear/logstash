package org.logstash.cluster.primitives.map.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * An unmodifiable {@link AsyncConsistentMap}.
 * <p>
 * Any attempt to update the map through this instance will cause the
 * operation to be completed with an {@link UnsupportedOperationException}.
 * @param <K> key type
 * @param <V> value type
 */
public class UnmodifiableAsyncConsistentMap<K, V> extends DelegatingAsyncConsistentMap<K, V> {

    private static final String ERROR_MSG = "map updates are not allowed";

    public UnmodifiableAsyncConsistentMap(AsyncConsistentMap<K, V> backingMap) {
        super(backingMap);
    }

    @Override
    public CompletableFuture<Void> clear() {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Versioned<V>> computeIf(K key,
        Predicate<? super V> condition,
        BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(""));
    }

    @Override
    public CompletableFuture<Versioned<V>> put(K key, V value) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Versioned<V>> remove(K key) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, V value) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, long version) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Versioned<V>> replace(K key, V value) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
        return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
    }
}

package org.logstash.cluster.primitives.map.impl;

import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.time.Versioned;

/**
 * {@link AsyncConsistentMap} that doesn't allow null values.
 */
public class NotNullAsyncConsistentMap<K, V> extends DelegatingAsyncConsistentMap<K, V> {

    public NotNullAsyncConsistentMap(AsyncConsistentMap<K, V> delegateMap) {
        super(delegateMap);
    }

    @Override
    public CompletableFuture<Boolean> containsValue(V value) {
        if (value == null) {
            return CompletableFuture.completedFuture(false);
        }
        return super.containsValue(value);
    }

    @Override
    public CompletableFuture<Versioned<V>> get(K key) {
        return super.get(key).thenApply(v -> v != null && v.value() == null ? null : v);
    }

    @Override
    public CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys) {
        return super.getAllPresent(keys).thenApply(m -> ImmutableMap.copyOf(m.entrySet()
            .stream().filter(e -> e.getValue().value() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Override
    public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
        return super.getOrDefault(key, defaultValue).thenApply(v -> v != null && v.value() == null ? null : v);
    }

    @Override
    public CompletableFuture<Versioned<V>> put(K key, V value) {
        if (value == null) {
            return super.remove(key);
        }
        return super.put(key, value);
    }

    @Override
    public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
        if (value == null) {
            return super.remove(key).thenApply(v -> null);
        }
        return super.putAndGet(key, value);
    }

    @Override
    public CompletableFuture<Collection<Versioned<V>>> values() {
        return super.values().thenApply(value -> value.stream()
            .filter(v -> v.value() != null)
            .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Set<Map.Entry<K, Versioned<V>>>> entrySet() {
        return super.entrySet().thenApply(entries -> entries.stream()
            .filter(e -> e.getValue().value() != null)
            .collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
        if (value == null) {
            return super.remove(key);
        }
        return super.putIfAbsent(key, value);
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, V value) {
        if (value == null) {
            return CompletableFuture.completedFuture(false);
        }
        return super.remove(key, value);
    }

    @Override
    public CompletableFuture<Boolean> remove(K key, long version) {
        return super.remove(key, version);
    }

    @Override
    public CompletableFuture<Versioned<V>> replace(K key, V value) {
        if (value == null) {
            return super.remove(key);
        }
        return super.replace(key, value);
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
        if (oldValue == null) {
            return super.putIfAbsent(key, newValue).thenApply(Objects::isNull);
        } else if (newValue == null) {
            return super.remove(key, oldValue);
        }
        return super.replace(key, oldValue, newValue);
    }

    @Override
    public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
        return super.replace(key, oldVersion, newValue);
    }
}

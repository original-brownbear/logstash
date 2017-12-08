package org.logstash.cluster.primitives.map.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.primitives.map.MapEventListener;
import org.logstash.cluster.time.Versioned;

/**
 * {@code AsyncConsistentMap} that caches entries on read.
 * <p>
 * The cache entries are automatically invalidated when updates are detected either locally or
 * remotely.
 * <p> This implementation only attempts to serve cached entries for {@link AsyncConsistentMap#get get}
 * {@link AsyncConsistentMap#getOrDefault(Object, Object) getOrDefault}, and
 * {@link AsyncConsistentMap#containsKey(Object) containsKey} calls. All other calls skip the cache
 * and directly go the backing map.
 * @param <K> key type
 * @param <V> value type
 */
public class CachingAsyncConsistentMap<K, V> extends DelegatingAsyncConsistentMap<K, V> {

    private static final Logger LOGGER = LogManager.getLogger(CachingAsyncConsistentMap.class);

    private static final int DEFAULT_CACHE_SIZE = 10000;

    private final LoadingCache<K, CompletableFuture<Versioned<V>>> cache;
    private final AsyncConsistentMap<K, V> backingMap;
    private final MapEventListener<K, V> cacheUpdater;
    private final Consumer<DistributedPrimitive.Status> statusListener;

    /**
     * Default constructor.
     * @param backingMap a distributed, strongly consistent map for backing
     */
    public CachingAsyncConsistentMap(final AsyncConsistentMap<K, V> backingMap) {
        this(backingMap, DEFAULT_CACHE_SIZE);
    }

    /**
     * Constructor to configure cache size.
     * @param backingMap a distributed, strongly consistent map for backing
     * @param cacheSize the maximum size of the cache
     */
    public CachingAsyncConsistentMap(final AsyncConsistentMap<K, V> backingMap, final int cacheSize) {
        super(backingMap);
        this.backingMap = backingMap;
        cache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .build(CacheLoader.from(CachingAsyncConsistentMap.super::get));
        cacheUpdater = event -> {
            final Versioned<V> newValue = event.newValue();
            if (newValue == null) {
                cache.invalidate(event.key());
            } else {
                cache.put(event.key(), CompletableFuture.completedFuture(newValue));
            }
        };
        statusListener = status -> {
            LOGGER.debug("{} status changed to {}", this.name(), status);
            // If the status of the underlying map is SUSPENDED or INACTIVE
            // we can no longer guarantee that the cache will be in sync.
            if (status == DistributedPrimitive.Status.SUSPENDED || status == DistributedPrimitive.Status.INACTIVE) {
                cache.invalidateAll();
            }
        };
        addListener(cacheUpdater);
        addStatusChangeListener(statusListener);
    }

    @Override
    public CompletableFuture<Void> destroy() {
        removeStatusChangeListener(statusListener);
        return super.destroy().thenCompose(v -> removeListener(cacheUpdater));
    }

    @Override
    public CompletableFuture<Boolean> containsKey(final K key) {
        return cache.getUnchecked(key).thenApply(Objects::nonNull)
            .whenComplete((r, e) -> {
                if (e != null) {
                    cache.invalidate(key);
                }
            });
    }

    @Override
    public CompletableFuture<Versioned<V>> get(final K key) {
        return cache.getUnchecked(key)
            .whenComplete((r, e) -> {
                if (e != null) {
                    cache.invalidate(key);
                }
            });
    }

    @Override
    public CompletableFuture<Versioned<V>> getOrDefault(final K key, final V defaultValue) {
        return cache.getUnchecked(key).thenCompose(r -> {
            if (r == null) {
                CompletableFuture<Versioned<V>> versioned = backingMap.getOrDefault(key, defaultValue);
                cache.put(key, versioned);
                return versioned;
            } else {
                return CompletableFuture.completedFuture(r);
            }
        }).whenComplete((r, e) -> {
            if (e != null) {
                cache.invalidate(key);
            }
        });
    }

    @Override
    public CompletableFuture<Versioned<V>> computeIf(final K key,
        final Predicate<? super V> condition,
        final BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return super.computeIf(key, condition, remappingFunction)
            .whenComplete((r, e) -> cache.invalidate(key));
    }

    @Override
    public CompletableFuture<Versioned<V>> put(final K key, final V value) {
        return super.put(key, value)
            .whenComplete((r, e) -> cache.invalidate(key));
    }

    @Override
    public CompletableFuture<Versioned<V>> putAndGet(final K key, final V value) {
        return super.putAndGet(key, value)
            .whenComplete((r, e) -> cache.invalidate(key));
    }

    @Override
    public CompletableFuture<Versioned<V>> remove(final K key) {
        return super.remove(key)
            .whenComplete((r, e) -> cache.invalidate(key));
    }

    @Override
    public CompletableFuture<Void> clear() {
        return super.clear()
            .whenComplete((r, e) -> cache.invalidateAll());
    }

    @Override
    public CompletableFuture<Versioned<V>> putIfAbsent(final K key, final V value) {
        return super.putIfAbsent(key, value)
            .whenComplete((r, e) -> cache.invalidate(key));
    }

    @Override
    public CompletableFuture<Boolean> remove(final K key, final V value) {
        return super.remove(key, value)
            .whenComplete((r, e) -> {
                if (r) {
                    cache.invalidate(key);
                }
            });
    }

    @Override
    public CompletableFuture<Boolean> remove(final K key, final long version) {
        return super.remove(key, version)
            .whenComplete((r, e) -> {
                if (r) {
                    cache.invalidate(key);
                }
            });
    }

    @Override
    public CompletableFuture<Versioned<V>> replace(final K key, final V value) {
        return super.replace(key, value)
            .whenComplete((r, e) -> cache.invalidate(key));
    }

    @Override
    public CompletableFuture<Boolean> replace(final K key, final V oldValue, final V newValue) {
        return super.replace(key, oldValue, newValue)
            .whenComplete((r, e) -> {
                if (r) {
                    cache.invalidate(key);
                }
            });
    }

    @Override
    public CompletableFuture<Boolean> replace(final K key, final long oldVersion, final V newValue) {
        return super.replace(key, oldVersion, newValue)
            .whenComplete((r, e) -> {
                if (r) {
                    cache.invalidate(key);
                }
            });
    }
}

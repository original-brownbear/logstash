package org.logstash.cluster.primitives.tree.impl;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.DocumentTreeListener;
import org.logstash.cluster.time.Versioned;

/**
 * Caching asynchronous document tree.
 */
public class CachingAsyncDocumentTree<V> extends DelegatingAsyncDocumentTree<V> {

    private static final Logger LOGGER = LogManager.getLogger(CachingAsyncDocumentTree.class);

    private static final int DEFAULT_CACHE_SIZE = 10000;

    private final LoadingCache<DocumentPath, CompletableFuture<Versioned<V>>> cache;
    private final DocumentTreeListener<V> cacheUpdater;
    private final Consumer<DistributedPrimitive.Status> statusListener;

    /**
     * Default constructor.
     * @param backingTree a distributed, strongly consistent map for backing
     */
    public CachingAsyncDocumentTree(AsyncDocumentTree<V> backingTree) {
        this(backingTree, DEFAULT_CACHE_SIZE);
    }

    /**
     * Constructor to configure cache size.
     * @param backingTree a distributed, strongly consistent map for backing
     * @param cacheSize the maximum size of the cache
     */
    public CachingAsyncDocumentTree(AsyncDocumentTree<V> backingTree, int cacheSize) {
        super(backingTree);
        cache = CacheBuilder.newBuilder()
            .maximumSize(cacheSize)
            .build(CacheLoader.from(CachingAsyncDocumentTree.super::get));
        cacheUpdater = event -> {
            if (!event.newValue().isPresent()) {
                cache.invalidate(event.path());
            } else {
                cache.put(event.path(), CompletableFuture.completedFuture(event.newValue().get()));
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
    public CompletableFuture<Versioned<V>> get(DocumentPath path) {
        return cache.getUnchecked(path);
    }

    @Override
    public CompletableFuture<Versioned<V>> set(DocumentPath path, V value) {
        return super.set(path, value)
            .whenComplete((r, e) -> cache.invalidate(path));
    }

    @Override
    public CompletableFuture<Boolean> create(DocumentPath path, V value) {
        return super.create(path, value)
            .whenComplete((r, e) -> cache.invalidate(path));
    }

    @Override
    public CompletableFuture<Boolean> createRecursive(DocumentPath path, V value) {
        return super.createRecursive(path, value)
            .whenComplete((r, e) -> cache.invalidate(path));
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, long version) {
        return super.replace(path, newValue, version)
            .whenComplete((r, e) -> {
                if (r) {
                    cache.invalidate(path);
                }
            });
    }

    @Override
    public CompletableFuture<Boolean> replace(DocumentPath path, V newValue, V currentValue) {
        return super.replace(path, newValue, currentValue)
            .whenComplete((r, e) -> {
                if (r) {
                    cache.invalidate(path);
                }
            });
    }

    @Override
    public CompletableFuture<Versioned<V>> removeNode(DocumentPath path) {
        return super.removeNode(path)
            .whenComplete((r, e) -> cache.invalidate(path));
    }
}

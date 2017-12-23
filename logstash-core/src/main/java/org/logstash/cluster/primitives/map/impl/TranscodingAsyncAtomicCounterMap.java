package org.logstash.cluster.primitives.map.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.logstash.cluster.primitives.map.AsyncAtomicCounterMap;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * An {@code AsyncAtomicCounterMap} that transcodes keys.
 */
public class TranscodingAsyncAtomicCounterMap<K1, K2> implements AsyncAtomicCounterMap<K1> {
    private final AsyncAtomicCounterMap<K2> backingMap;
    private final Function<K1, K2> keyEncoder;
    private final Function<K2, K1> keyDecoder;

    public TranscodingAsyncAtomicCounterMap(AsyncAtomicCounterMap<K2> backingMap,
        Function<K1, K2> keyEncoder, Function<K2, K1> keyDecoder) {
        this.backingMap = backingMap;
        this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
        this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
    }

    @Override
    public String name() {
        return backingMap.name();
    }

    @Override
    public CompletableFuture<Long> incrementAndGet(K1 key) {
        try {
            return backingMap.incrementAndGet(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> decrementAndGet(K1 key) {
        try {
            return backingMap.decrementAndGet(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> getAndIncrement(K1 key) {
        try {
            return backingMap.getAndIncrement(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> getAndDecrement(K1 key) {
        try {
            return backingMap.getAndDecrement(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> addAndGet(K1 key, long delta) {
        try {
            return backingMap.addAndGet(keyEncoder.apply(key), delta);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> getAndAdd(K1 key, long delta) {
        try {
            return backingMap.getAndAdd(keyEncoder.apply(key), delta);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> get(K1 key) {
        try {
            return backingMap.get(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> put(K1 key, long newValue) {
        try {
            return backingMap.put(keyEncoder.apply(key), newValue);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> putIfAbsent(K1 key, long newValue) {
        try {
            return backingMap.putIfAbsent(keyEncoder.apply(key), newValue);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> replace(K1 key, long expectedOldValue, long newValue) {
        try {
            return backingMap.replace(keyEncoder.apply(key), expectedOldValue, newValue);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Long> remove(K1 key) {
        try {
            return backingMap.remove(keyEncoder.apply(key));
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> remove(K1 key, long value) {
        try {
            return backingMap.remove(keyEncoder.apply(key), value);
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Integer> size() {
        try {
            return backingMap.size();
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
        try {
            return backingMap.isEmpty();
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> clear() {
        try {
            return backingMap.clear();
        } catch (Exception e) {
            return Futures.exceptionalFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> close() {
        return backingMap.close();
    }
}

package org.logstash.cluster.primitives.map;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.primitives.map.impl.BlockingConsistentTreeMap;
import org.logstash.cluster.time.Versioned;

/**
 * API for a distributed tree map implementation.
 */
public interface AsyncConsistentTreeMap<K, V> extends AsyncConsistentMap<K, V> {

    /**
     * Return the lowest key in the map.
     * @return the key or null if none exist
     */
    CompletableFuture<K> firstKey();

    /**
     * Return the highest key in the map.
     * @return the key or null if none exist
     */
    CompletableFuture<K> lastKey();

    /**
     * Returns the entry associated with the least key greater than or equal to
     * the key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> ceilingEntry(K key);

    /**
     * Returns the entry associated with the greatest key less than or equal
     * to key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> floorEntry(K key);

    /**
     * Returns the entry associated with the least key greater than key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> higherEntry(K key);

    /**
     * Returns the entry associated with the largest key less than key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> lowerEntry(K key);

    /**
     * Return the entry associated with the lowest key in the map.
     * @return the entry or null if none exist
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> firstEntry();

    /**
     * Return the entry associated with the highest key in the map.
     * @return the entry or null if none exist
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> lastEntry();

    /**
     * Return and remove the entry associated with the lowest key.
     * @return the entry or null if none exist
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> pollFirstEntry();

    /**
     * Return and remove the entry associated with the highest key.
     * @return the entry or null if none exist
     */
    CompletableFuture<Map.Entry<K, Versioned<V>>> pollLastEntry();

    /**
     * Return the entry associated with the greatest key less than key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<K> lowerKey(K key);

    /**
     * Return the highest key less than or equal to key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<K> floorKey(K key);

    /**
     * Return the lowest key greater than or equal to key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<K> ceilingKey(K key);

    /**
     * Return the lowest key greater than key.
     * @param key the key
     * @return the entry or null if no suitable key exists
     */
    CompletableFuture<K> higherKey(K key);

    /**
     * Returns a navigable set of the keys in this map.
     * @return a navigable key set (this may be empty)
     */
    CompletableFuture<NavigableSet<K>> navigableKeySet();

    /**
     * Returns a navigable map containing the entries from the original map
     * which are larger than (or if specified equal to) {@code lowerKey} AND
     * less than (or if specified equal to) {@code upperKey}.
     * @param upperKey the upper bound for the keys in this map
     * @param lowerKey the lower bound for the keys in this map
     * @param inclusiveUpper whether keys equal to the upperKey should be
     * included
     * @param inclusiveLower whether keys equal to the lowerKey should be
     * included
     * @return a navigable map containing entries in the specified range (this
     * may be empty)
     */
    CompletableFuture<NavigableMap<K, V>> subMap(K upperKey,
        K lowerKey,
        boolean inclusiveUpper,
        boolean inclusiveLower);

    default ConsistentTreeMap<K, V> asTreeMap() {
        return asTreeMap(DEFAULT_OPERATION_TIMEOUT_MILLIS);
    }

    default ConsistentTreeMap<K, V> asTreeMap(long timeoutMillis) {
        return new BlockingConsistentTreeMap<>(this, timeoutMillis);
    }

}

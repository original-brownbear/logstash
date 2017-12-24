package org.logstash.cluster.primitives.map;

/**
 * Listener to be notified about updates to a ConsistentMap.
 */
public interface MapEventListener<K, V> {
    /**
     * Reacts to the specified event.
     * @param event the event
     */
    void event(MapEvent<K, V> event);
}

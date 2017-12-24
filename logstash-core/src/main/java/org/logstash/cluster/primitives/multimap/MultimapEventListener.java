package org.logstash.cluster.primitives.multimap;

/**
 * Listener to be notified about updates to a ConsistentMultimap.
 */
public interface MultimapEventListener<K, V> {

    /**
     * Reacts to the specified event.
     * @param event the event
     */
    void event(MultimapEvent<K, V> event);
}

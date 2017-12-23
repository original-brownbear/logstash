package org.logstash.cluster.primitives.value;

/**
 * Listener to be notified about updates to a AtomicValue.
 */
public interface AtomicValueEventListener<V> {
    /**
     * Reacts to the specified event.
     * @param event the event
     */
    void event(AtomicValueEvent<V> event);
}

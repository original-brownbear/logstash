package org.logstash.cluster.event;

/**
 * Entity capable of receiving events.
 */
@FunctionalInterface
public interface EventListener<E extends Event> extends EventFilter<E> {

    /**
     * Reacts to the specified event.
     * @param event event to be processed
     */
    void onEvent(E event);

}

package org.logstash.cluster.primitives.set;

import java.util.Set;
import org.logstash.cluster.primitives.SyncPrimitive;

/**
 * A distributed collection designed for holding unique elements.
 * @param <E> set entry type
 */
public interface DistributedSet<E> extends Set<E>, SyncPrimitive {

    /**
     * Registers the specified listener to be notified whenever
     * the set is updated.
     * @param listener listener to notify about set update events
     */
    void addListener(SetEventListener<E> listener);

    /**
     * Unregisters the specified listener.
     * @param listener listener to unregister.
     */
    void removeListener(SetEventListener<E> listener);
}

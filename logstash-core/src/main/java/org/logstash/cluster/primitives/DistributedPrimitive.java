package org.logstash.cluster.primitives;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

/**
 * Interface for all distributed primitives.
 */
public interface DistributedPrimitive {

    /**
     * Default timeout for primitive operations.
     */
    long DEFAULT_OPERATION_TIMEOUT_MILLIS = 5000L;

    /**
     * Returns the name of this primitive.
     * @return name
     */
    String name();

    /**
     * Returns the type of primitive.
     * @return primitive type
     */
    Type primitiveType();

    /**
     * Registers a listener to be called when the primitive's status changes.
     * @param listener The listener to be called when the status changes.
     */
    default void addStatusChangeListener(Consumer<Status> listener) {
    }

    /**
     * Unregisters a previously registered listener to be called when the primitive's status changes.
     * @param listener The listener to unregister
     */
    default void removeStatusChangeListener(Consumer<Status> listener) {
    }

    /**
     * Returns the collection of status change listeners previously registered.
     * @return collection of status change listeners
     */
    default Collection<Consumer<Status>> statusChangeListeners() {
        return Collections.emptyList();
    }

    /**
     * Type of distributed primitive.
     */
    enum Type {
        /**
         * Map with strong consistency semantics.
         */
        CONSISTENT_MAP,

        /**
         * Consistent Multimap.
         */
        CONSISTENT_MULTIMAP,

        /**
         * Tree map.
         */
        CONSISTENT_TREEMAP,

        /**
         * Distributed set.
         */
        SET,

        /**
         * Atomic counter.
         */
        COUNTER,

        /**
         * Numeric ID generator.
         */
        ID_GENERATOR,

        /**
         * Atomic counter map.
         */
        COUNTER_MAP,

        /**
         * Atomic value.
         */
        VALUE,

        /**
         * Distributed work queue.
         */
        WORK_QUEUE,

        /**
         * Document tree.
         */
        DOCUMENT_TREE,

        /**
         * Distributed topic.
         */
        TOPIC,

        /**
         * Leader elector.
         */
        LEADER_ELECTOR,

        /**
         * Lock.
         */
        LOCK,

        /**
         * Transaction Context.
         */
        TRANSACTION_CONTEXT
    }

    /**
     * Status of distributed primitive.
     */
    enum Status {

        /**
         * Signifies a state wherein the primitive is operating correctly and is capable of meeting the advertised
         * consistency and reliability guarantees.
         */
        ACTIVE,

        /**
         * Signifies a state wherein the primitive is temporarily incapable of providing the advertised
         * consistency properties.
         */
        SUSPENDED,

        /**
         * Signifies a state wherein the primitive has been shutdown and therefore cannot perform its functions.
         */
        INACTIVE
    }

}

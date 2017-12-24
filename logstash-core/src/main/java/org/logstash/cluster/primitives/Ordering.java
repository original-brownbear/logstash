package org.logstash.cluster.primitives;

/**
 * Describes the order of a primitive data structure.
 */
public enum Ordering {

    /**
     * Indicates that items should be ordered in their natural order.
     */
    NATURAL,

    /**
     * Indicates that items should be ordered in insertion order.
     */
    INSERTION,
}

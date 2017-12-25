package org.logstash.cluster.utils;

/**
 * Abstract identifier backed by another value, e.g. string, int.
 */
public interface Identifier<T extends Comparable<T>> {

    /**
     * Returns the backing identifier value.
     * @return identifier
     */
    T id();
}

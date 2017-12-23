package org.logstash.cluster.utils;

/**
 * Object builder.
 * <p>
 * This is a base interface for building objects in Catalyst.
 * @param <T> type to build
 */
public interface Builder<T> {

    /**
     * Builds the object.
     * <p>
     * The returned object may be a new instance of the built class or a recycled instance, depending on the semantics
     * of the builder implementation. Users should never assume that a builder allocates a new instance.
     * @return The built object.
     */
    T build();

}

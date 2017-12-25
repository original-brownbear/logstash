package org.logstash.cluster.utils.concurrent;

/**
 * Reference manager. Manages {@link ReferenceCounted} objects.
 */
public interface ReferenceManager<T> {

    /**
     * Releases the given reference.
     * <p>
     * This method should be called with a {@link ReferenceCounted} object that contains no
     * additional references. This allows, for instance, pools to recycle dereferenced objects.
     * @param reference The reference to release.
     */
    void release(T reference);

}

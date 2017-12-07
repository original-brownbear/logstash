package org.logstash.cluster.primitives;

/**
 * Interface for mapping from an object to partition ID.
 * @param <K> object type.
 */
public interface Hasher<K> {

    /**
     * Returns the partition ID to which the specified object maps.
     * @param object object
     * @return partition identifier
     */
    int hash(K object);

}

package org.logstash.cluster.storage;

/**
 * Storage level configuration values which control how logs are stored on disk or in memory.
 */
public enum StorageLevel {

    /**
     * Stores data in memory only.
     */
    MEMORY,

    /**
     * Stores data in a memory-mapped file.
     */
    MAPPED,

    /**
     * Stores data on disk.
     */
    DISK

}

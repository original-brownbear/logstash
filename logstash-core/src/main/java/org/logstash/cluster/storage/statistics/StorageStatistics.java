package org.logstash.cluster.storage.statistics;

import java.io.File;

/**
 * Atomix storage statistics.
 */
public class StorageStatistics {
    private final File file;

    public StorageStatistics(File file) {
        this.file = file;
    }

    /**
     * Returns the amount of usable space remaining.
     * @return the amount of usable space remaining
     */
    public long getUsableSpace() {
        return file.getUsableSpace();
    }

    /**
     * Returns the amount of free space remaining.
     * @return the amount of free space remaining
     */
    public long getFreeSpace() {
        return file.getFreeSpace();
    }

    /**
     * Returns the total amount of space.
     * @return the total amount of space
     */
    public long getTotalSpace() {
        return file.getTotalSpace();
    }
}

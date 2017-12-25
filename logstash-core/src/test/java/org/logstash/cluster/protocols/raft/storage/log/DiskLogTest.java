package org.logstash.cluster.protocols.raft.storage.log;

import org.logstash.cluster.storage.StorageLevel;

/**
 * Disk log test.
 */
public class DiskLogTest extends PersistentLogTest {
    @Override
    protected StorageLevel storageLevel() {
        return StorageLevel.DISK;
    }
}

package org.logstash.cluster.protocols.raft.storage.log;

import org.logstash.cluster.storage.StorageLevel;

/**
 * Memory log test.
 */
public class MemoryLogTest extends AbstractLogTest {
    @Override
    protected StorageLevel storageLevel() {
        return StorageLevel.MEMORY;
    }
}

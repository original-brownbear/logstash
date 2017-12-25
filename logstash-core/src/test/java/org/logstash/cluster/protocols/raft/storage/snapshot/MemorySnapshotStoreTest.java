package org.logstash.cluster.protocols.raft.storage.snapshot;

import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.storage.StorageLevel;

/**
 * Memory snapshot store test.
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MemorySnapshotStoreTest extends AbstractSnapshotStoreTest {

    /**
     * Returns a new snapshot store.
     */
    protected SnapshotStore createSnapshotStore() {
        RaftStorage storage = RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build();
        return new SnapshotStore(storage);
    }

}

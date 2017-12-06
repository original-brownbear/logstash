package org.logstash.cluster.protocols.raft.storage.snapshot;

import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.logstash.cluster.protocols.raft.service.ServiceId;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.time.WallClockTimestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * File snapshot store test.
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class FileSnapshotStoreTest extends AbstractSnapshotStoreTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String testId;

    /**
     * Tests storing and loading snapshots.
     */
    @Test
    public void testStoreLoadSnapshot() {
        SnapshotStore store = createSnapshotStore();

        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "foo", 2, new WallClockTimestamp());
        try (SnapshotWriter writer = snapshot.openWriter()) {
            writer.writeLong(10);
        }
        snapshot.complete();
        assertNotNull(store.getSnapshotById(ServiceId.from(1)));
        assertNotNull(store.getSnapshotsByIndex(2));
        store.close();

        store = createSnapshotStore();
        assertNotNull(store.getSnapshotById(ServiceId.from(1)));
        assertNotNull(store.getSnapshotsByIndex(2));
        assertEquals(store.getSnapshotById(ServiceId.from(1)).serviceId(), ServiceId.from(1));
        assertEquals(store.getSnapshotById(ServiceId.from(1)).serviceName(), "foo");
        assertEquals(store.getSnapshotById(ServiceId.from(1)).index(), 2);
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().serviceId(), ServiceId.from(1));
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().serviceName(), "foo");
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().index(), 2);

        try (SnapshotReader reader = snapshot.openReader()) {
            assertEquals(reader.readLong(), 10);
        }
    }

    /**
     * Returns a new snapshot store.
     */
    protected SnapshotStore createSnapshotStore() {
        RaftStorage storage = RaftStorage.builder()
            .withPrefix("test")
            .withDirectory(
                temporaryFolder.getRoot().toPath().resolve("test-logs").resolve(testId).toFile()
            )
            .withStorageLevel(StorageLevel.DISK)
            .build();
        return new SnapshotStore(storage);
    }

    /**
     * Tests persisting and loading snapshots.
     */
    @Test
    public void testPersistLoadSnapshot() {
        SnapshotStore store = createSnapshotStore();

        Snapshot snapshot = store.newTemporarySnapshot(ServiceId.from(1), "foo", 2, new WallClockTimestamp());
        try (SnapshotWriter writer = snapshot.openWriter()) {
            writer.writeLong(10);
        }

        snapshot = snapshot.persist();

        assertNull(store.getSnapshotById(ServiceId.from(1)));
        assertNull(store.getSnapshotsByIndex(2));

        snapshot.complete();
        assertNotNull(store.getSnapshotById(ServiceId.from(1)));
        assertNotNull(store.getSnapshotsByIndex(2));

        try (SnapshotReader reader = snapshot.openReader()) {
            assertEquals(reader.readLong(), 10);
        }

        store.close();

        store = createSnapshotStore();
        assertNotNull(store.getSnapshotById(ServiceId.from(1)));
        assertNotNull(store.getSnapshotsByIndex(2));
        assertEquals(store.getSnapshotById(ServiceId.from(1)).serviceId(), ServiceId.from(1));
        assertEquals(store.getSnapshotById(ServiceId.from(1)).serviceName(), "foo");
        assertEquals(store.getSnapshotById(ServiceId.from(1)).index(), 2);
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().serviceId(), ServiceId.from(1));
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().serviceName(), "foo");
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().index(), 2);

        snapshot = store.getSnapshotById(ServiceId.from(1));
        try (SnapshotReader reader = snapshot.openReader()) {
            assertEquals(reader.readLong(), 10);
        }
    }

    @Before
    @After
    public void cleanupStorage() {
        testId = UUID.randomUUID().toString();
    }

}

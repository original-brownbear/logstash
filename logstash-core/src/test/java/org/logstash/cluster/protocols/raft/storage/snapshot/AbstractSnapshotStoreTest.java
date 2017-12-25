package org.logstash.cluster.protocols.raft.storage.snapshot;

import org.junit.Test;
import org.logstash.cluster.protocols.raft.service.ServiceId;
import org.logstash.cluster.time.WallClockTimestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Snapshot store test.
 */
public abstract class AbstractSnapshotStoreTest {

    /**
     * Tests writing a snapshot.
     */
    @Test
    public void testWriteSnapshotChunks() {
        SnapshotStore store = createSnapshotStore();
        WallClockTimestamp timestamp = new WallClockTimestamp();
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "foo", 2, timestamp);
        assertEquals(snapshot.serviceId(), ServiceId.from(1));
        assertEquals(snapshot.index(), 2);
        assertEquals(snapshot.timestamp(), timestamp);

        assertNull(store.getSnapshotById(ServiceId.from(1)));
        assertNull(store.getSnapshotsByIndex(2));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            writer.writeLong(10);
        }

        assertNull(store.getSnapshotById(ServiceId.from(1)));
        assertNull(store.getSnapshotsByIndex(2));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            writer.writeLong(11);
        }

        assertNull(store.getSnapshotById(ServiceId.from(1)));
        assertNull(store.getSnapshotsByIndex(2));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            writer.writeLong(12);
        }

        assertNull(store.getSnapshotById(ServiceId.from(1)));
        assertNull(store.getSnapshotsByIndex(2));
        snapshot.complete();

        assertEquals(store.getSnapshotById(ServiceId.from(1)).serviceId(), ServiceId.from(1));
        assertEquals(store.getSnapshotById(ServiceId.from(1)).index(), 2);
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().serviceId(), ServiceId.from(1));
        assertEquals(store.getSnapshotsByIndex(2).iterator().next().index(), 2);

        try (SnapshotReader reader = store.getSnapshotById(ServiceId.from(1)).openReader()) {
            assertEquals(reader.readLong(), 10);
            assertEquals(reader.readLong(), 11);
            assertEquals(reader.readLong(), 12);
        }
    }

    /**
     * Returns a new snapshot store.
     */
    protected abstract SnapshotStore createSnapshotStore();

}

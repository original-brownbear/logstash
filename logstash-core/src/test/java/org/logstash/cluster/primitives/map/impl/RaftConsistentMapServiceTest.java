package org.logstash.cluster.primitives.map.impl;

import org.junit.Test;
import org.logstash.cluster.protocols.raft.service.ServiceId;
import org.logstash.cluster.protocols.raft.service.impl.DefaultCommit;
import org.logstash.cluster.protocols.raft.session.impl.RaftSessionContext;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.protocols.raft.storage.snapshot.Snapshot;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotStore;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.time.WallClockTimestamp;
import org.mockito.Mockito;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Consistent map service test.
 */
public class RaftConsistentMapServiceTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshot() {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        RaftConsistentMapService service = new RaftConsistentMapService();
        service.put(new DefaultCommit<>(
            2,
            RaftConsistentMapOperations.PUT,
            new RaftConsistentMapOperations.Put("foo", "Hello world!".getBytes()),
            Mockito.mock(RaftSessionContext.class),
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftConsistentMapService();
        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        Versioned<byte[]> value = service.get(new DefaultCommit<>(
            2,
            RaftConsistentMapOperations.GET,
            new RaftConsistentMapOperations.Get("foo"),
            Mockito.mock(RaftSessionContext.class),
            System.currentTimeMillis()));
        assertNotNull(value);
        assertArrayEquals("Hello world!".getBytes(), value.value());
    }
}

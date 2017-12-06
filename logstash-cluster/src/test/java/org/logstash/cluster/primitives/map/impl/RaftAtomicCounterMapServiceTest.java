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
import org.logstash.cluster.time.WallClockTimestamp;

import static org.junit.Assert.assertEquals;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.GET;
import static org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations.PUT;
import static org.mockito.Mockito.mock;

/**
 * Atomic counter map service test.
 */
public class RaftAtomicCounterMapServiceTest {
    @Test
    public void testSnapshot() throws Exception {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        RaftAtomicCounterMapService service = new RaftAtomicCounterMapService();
        service.put(new DefaultCommit<>(
            2,
            PUT,
            new RaftAtomicCounterMapOperations.Put("foo", 1),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftAtomicCounterMapService();
        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        long value = service.get(new DefaultCommit<>(
            2,
            GET,
            new RaftAtomicCounterMapOperations.Get("foo"),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));
        assertEquals(1, value);
    }
}

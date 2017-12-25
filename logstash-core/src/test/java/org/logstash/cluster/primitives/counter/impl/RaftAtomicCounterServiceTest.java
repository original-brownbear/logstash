package org.logstash.cluster.primitives.counter.impl;

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
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.GET;
import static org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations.SET;
import static org.mockito.Mockito.mock;

/**
 * Counter service test.
 */
public class RaftAtomicCounterServiceTest {
    @Test
    public void testSnapshot() {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        RaftAtomicCounterService service = new RaftAtomicCounterService();
        service.set(new DefaultCommit<>(
            2,
            SET,
            new RaftAtomicCounterOperations.Set(1L),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftAtomicCounterService();
        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        long value = service.get(new DefaultCommit<>(
            2,
            GET,
            null,
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));
        assertEquals(1, value);
    }
}

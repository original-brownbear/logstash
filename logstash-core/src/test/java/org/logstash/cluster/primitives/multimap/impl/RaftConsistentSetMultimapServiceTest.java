package org.logstash.cluster.primitives.multimap.impl;

import java.util.Arrays;
import java.util.Collection;
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
import org.logstash.cluster.utils.Match;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.logstash.cluster.primitives.multimap.impl.RaftConsistentSetMultimapOperations.GET;
import static org.logstash.cluster.primitives.multimap.impl.RaftConsistentSetMultimapOperations.PUT;
import static org.mockito.Mockito.mock;

/**
 * Consistent set multimap service test.
 */
public class RaftConsistentSetMultimapServiceTest {
    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshot() {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        RaftConsistentSetMultimapService service = new RaftConsistentSetMultimapService();
        service.put(new DefaultCommit<>(
            2,
            PUT,
            new RaftConsistentSetMultimapOperations.Put(
                "foo", Arrays.asList("Hello world!".getBytes()), Match.ANY),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftConsistentSetMultimapService();
        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        Versioned<Collection<? extends byte[]>> value = service.get(new DefaultCommit<>(
            2,
            GET,
            new RaftConsistentSetMultimapOperations.Get("foo"),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));
        assertNotNull(value);
        assertEquals(1, value.value().size());
        assertArrayEquals("Hello world!".getBytes(), value.value().iterator().next());
    }
}

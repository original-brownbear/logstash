package org.logstash.cluster.primitives.tree.impl;

import java.util.Optional;
import org.junit.Test;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.tree.DocumentPath;
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
import static org.junit.Assert.assertNotNull;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.GET;
import static org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations.UPDATE;
import static org.mockito.Mockito.mock;

/**
 * Document tree service test.
 */
public class RaftDocumentTreeServiceTest {

    @Test
    public void testNaturalOrderedSnapshot() {
        testSnapshot(Ordering.NATURAL);
    }

    private void testSnapshot(Ordering ordering) {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        RaftDocumentTreeService service = new RaftDocumentTreeService(ordering);
        service.update(new DefaultCommit<>(
            2,
            UPDATE,
            new RaftDocumentTreeOperations.Update(
                DocumentPath.from("root|foo"),
                Optional.of("Hello world!".getBytes()),
                Match.any(),
                Match.ifNull()),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftDocumentTreeService(ordering);
        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        Versioned<byte[]> value = service.get(new DefaultCommit<>(
            2,
            GET,
            new RaftDocumentTreeOperations.Get(DocumentPath.from("root|foo")),
            mock(RaftSessionContext.class),
            System.currentTimeMillis()));
        assertNotNull(value);
        assertArrayEquals("Hello world!".getBytes(), value.value());
    }

    @Test
    public void testInsertionOrderedSnapshot() {
        testSnapshot(Ordering.INSERTION);
    }
}

package org.logstash.cluster.primitives.queue.impl;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.impl.RaftContext;
import org.logstash.cluster.protocols.raft.protocol.RaftServerProtocol;
import org.logstash.cluster.protocols.raft.service.ServiceId;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.service.impl.DefaultCommit;
import org.logstash.cluster.protocols.raft.service.impl.DefaultServiceContext;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.protocols.raft.session.impl.RaftSessionContext;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.protocols.raft.storage.snapshot.Snapshot;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotStore;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.concurrent.AtomixThreadFactory;
import org.logstash.cluster.utils.concurrent.SingleThreadContextFactory;
import org.logstash.cluster.utils.concurrent.ThreadContext;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.logstash.cluster.primitives.DistributedPrimitive.Type.WORK_QUEUE;
import static org.logstash.cluster.primitives.queue.impl.RaftWorkQueueOperations.ADD;
import static org.logstash.cluster.primitives.queue.impl.RaftWorkQueueOperations.TAKE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Work queue service test.
 */
public class RaftWorkQueueServiceTest {
    @Test
    public void testSnapshot() throws Exception {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        DefaultServiceContext context = mock(DefaultServiceContext.class);
        when(context.serviceType()).thenReturn(ServiceType.from(WORK_QUEUE.name()));
        when(context.serviceName()).thenReturn("test");
        when(context.serviceId()).thenReturn(ServiceId.from(1));
        when(context.executor()).thenReturn(mock(ThreadContext.class));

        RaftContext server = mock(RaftContext.class);
        when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));

        RaftSessionContext session = new RaftSessionContext(
            SessionId.from(1),
            MemberId.from("1"),
            "test",
            ServiceType.from(WORK_QUEUE.name()),
            ReadConsistency.LINEARIZABLE,
            100,
            5000,
            context,
            server,
            new SingleThreadContextFactory(new AtomixThreadFactory()));

        RaftWorkQueueService service = new RaftWorkQueueService();
        service.init(context);

        service.add(new DefaultCommit<>(
            2,
            ADD,
            new RaftWorkQueueOperations.Add(Arrays.asList("Hello world!".getBytes())),
            session,
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftWorkQueueService();
        service.init(context);

        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        Collection<Task<byte[]>> value = service.take(new DefaultCommit<>(
            2,
            TAKE,
            new RaftWorkQueueOperations.Take(1),
            session,
            System.currentTimeMillis()));
        assertNotNull(value);
        assertEquals(1, value.size());
        assertArrayEquals("Hello world!".getBytes(), value.iterator().next().payload());
    }
}

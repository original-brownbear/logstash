package org.logstash.cluster.primitives.leadership.impl;

import org.junit.Test;
import org.logstash.cluster.primitives.leadership.Leadership;
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
import org.logstash.cluster.time.WallClock;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.concurrent.AtomixThreadFactory;
import org.logstash.cluster.utils.concurrent.SingleThreadContextFactory;
import org.logstash.cluster.utils.concurrent.ThreadContext;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.logstash.cluster.primitives.DistributedPrimitive.Type.LEADER_ELECTOR;
import static org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations.RUN;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Leader elector service test.
 */
public class RaftLeaderElectorServiceTest {
    @Test
    public void testSnapshot() {
        SnapshotStore store = new SnapshotStore(RaftStorage.builder()
            .withPrefix("test")
            .withStorageLevel(StorageLevel.MEMORY)
            .build());
        Snapshot snapshot = store.newSnapshot(ServiceId.from(1), "test", 2, new WallClockTimestamp());

        DefaultServiceContext context = mock(DefaultServiceContext.class);
        when(context.serviceType()).thenReturn(ServiceType.from(LEADER_ELECTOR.name()));
        when(context.serviceName()).thenReturn("test");
        when(context.serviceId()).thenReturn(ServiceId.from(1));
        when(context.executor()).thenReturn(mock(ThreadContext.class));
        when(context.wallClock()).thenReturn(new WallClock());

        RaftContext server = mock(RaftContext.class);
        when(server.getProtocol()).thenReturn(mock(RaftServerProtocol.class));

        RaftLeaderElectorService service = new RaftLeaderElectorService();
        service.init(context);

        byte[] id = "a".getBytes();
        service.run(new DefaultCommit<>(
            2,
            RUN,
            new RaftLeaderElectorOperations.Run(id),
            new RaftSessionContext(
                SessionId.from(1),
                MemberId.from("1"),
                "test",
                ServiceType.from(LEADER_ELECTOR.name()),
                ReadConsistency.LINEARIZABLE,
                100,
                5000,
                context,
                server,
                new SingleThreadContextFactory(new AtomixThreadFactory())),
            System.currentTimeMillis()));

        try (SnapshotWriter writer = snapshot.openWriter()) {
            service.snapshot(writer);
        }

        snapshot.complete();

        service = new RaftLeaderElectorService();
        service.init(context);

        try (SnapshotReader reader = snapshot.openReader()) {
            service.install(reader);
        }

        Leadership<byte[]> value = service.getLeadership();
        assertNotNull(value);
        assertArrayEquals(value.leader().id(), id);
    }
}

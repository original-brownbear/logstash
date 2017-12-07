package org.logstash.cluster.partition.impl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterOperations;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorEvents;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorOperations;
import org.logstash.cluster.primitives.lock.impl.RaftDistributedLockEvents;
import org.logstash.cluster.primitives.lock.impl.RaftDistributedLockOperations;
import org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapOperations;
import org.logstash.cluster.primitives.map.impl.RaftConsistentMapEvents;
import org.logstash.cluster.primitives.map.impl.RaftConsistentMapOperations;
import org.logstash.cluster.primitives.map.impl.RaftConsistentTreeMapOperations;
import org.logstash.cluster.primitives.multimap.impl.RaftConsistentSetMultimapEvents;
import org.logstash.cluster.primitives.multimap.impl.RaftConsistentSetMultimapOperations;
import org.logstash.cluster.primitives.queue.impl.RaftWorkQueueEvents;
import org.logstash.cluster.primitives.queue.impl.RaftWorkQueueOperations;
import org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeEvents;
import org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeOperations;
import org.logstash.cluster.primitives.value.impl.RaftAtomicValueEvents;
import org.logstash.cluster.primitives.value.impl.RaftAtomicValueOperations;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;
import org.logstash.cluster.protocols.raft.cluster.impl.DefaultRaftMember;
import org.logstash.cluster.protocols.raft.event.RaftEvent;
import org.logstash.cluster.protocols.raft.event.impl.DefaultEventType;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.protocols.raft.operation.RaftOperation;
import org.logstash.cluster.protocols.raft.operation.impl.DefaultOperationId;
import org.logstash.cluster.protocols.raft.protocol.AppendRequest;
import org.logstash.cluster.protocols.raft.protocol.AppendResponse;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.CommandRequest;
import org.logstash.cluster.protocols.raft.protocol.CommandResponse;
import org.logstash.cluster.protocols.raft.protocol.ConfigureRequest;
import org.logstash.cluster.protocols.raft.protocol.ConfigureResponse;
import org.logstash.cluster.protocols.raft.protocol.HeartbeatRequest;
import org.logstash.cluster.protocols.raft.protocol.HeartbeatResponse;
import org.logstash.cluster.protocols.raft.protocol.InstallRequest;
import org.logstash.cluster.protocols.raft.protocol.InstallResponse;
import org.logstash.cluster.protocols.raft.protocol.JoinRequest;
import org.logstash.cluster.protocols.raft.protocol.JoinResponse;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveRequest;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveResponse;
import org.logstash.cluster.protocols.raft.protocol.LeaveRequest;
import org.logstash.cluster.protocols.raft.protocol.LeaveResponse;
import org.logstash.cluster.protocols.raft.protocol.MetadataRequest;
import org.logstash.cluster.protocols.raft.protocol.MetadataResponse;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.PollRequest;
import org.logstash.cluster.protocols.raft.protocol.PollResponse;
import org.logstash.cluster.protocols.raft.protocol.PublishRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureRequest;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureResponse;
import org.logstash.cluster.protocols.raft.protocol.ResetRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteResponse;
import org.logstash.cluster.protocols.raft.session.RaftSessionMetadata;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.protocols.raft.storage.log.entry.CloseSessionEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.CommandEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.ConfigurationEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.InitializeEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.KeepAliveEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.MetadataEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.OpenSessionEntry;
import org.logstash.cluster.protocols.raft.storage.log.entry.QueryEntry;
import org.logstash.cluster.protocols.raft.storage.system.Configuration;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Storage serializer namespaces.
 */
public final class RaftNamespaces {

    /**
     * Raft protocol namespace.
     */
    public static final KryoNamespace RAFT_PROTOCOL = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(OpenSessionRequest.class)
        .register(OpenSessionResponse.class)
        .register(CloseSessionRequest.class)
        .register(CloseSessionResponse.class)
        .register(KeepAliveRequest.class)
        .register(KeepAliveResponse.class)
        .register(HeartbeatRequest.class)
        .register(HeartbeatResponse.class)
        .register(QueryRequest.class)
        .register(QueryResponse.class)
        .register(CommandRequest.class)
        .register(CommandResponse.class)
        .register(MetadataRequest.class)
        .register(MetadataResponse.class)
        .register(JoinRequest.class)
        .register(JoinResponse.class)
        .register(LeaveRequest.class)
        .register(LeaveResponse.class)
        .register(ConfigureRequest.class)
        .register(ConfigureResponse.class)
        .register(ReconfigureRequest.class)
        .register(ReconfigureResponse.class)
        .register(InstallRequest.class)
        .register(InstallResponse.class)
        .register(PollRequest.class)
        .register(PollResponse.class)
        .register(VoteRequest.class)
        .register(VoteResponse.class)
        .register(AppendRequest.class)
        .register(AppendResponse.class)
        .register(PublishRequest.class)
        .register(ResetRequest.class)
        .register(RaftResponse.Status.class)
        .register(RaftError.class)
        .register(RaftError.Type.class)
        .register(ReadConsistency.class)
        .register(RaftSessionMetadata.class)
        .register(CloseSessionEntry.class)
        .register(CommandEntry.class)
        .register(ConfigurationEntry.class)
        .register(InitializeEntry.class)
        .register(KeepAliveEntry.class)
        .register(MetadataEntry.class)
        .register(OpenSessionEntry.class)
        .register(QueryEntry.class)
        .register(RaftOperation.class)
        .register(RaftEvent.class)
        .register(DefaultEventType.class)
        .register(DefaultOperationId.class)
        .register(OperationType.class)
        .register(ReadConsistency.class)
        .register(ArrayList.class)
        .register(LinkedList.class)
        .register(Collections.emptyList().getClass())
        .register(HashSet.class)
        .register(DefaultRaftMember.class)
        .register(MemberId.class)
        .register(SessionId.class)
        .register(RaftMember.Type.class)
        .register(Instant.class)
        .register(Configuration.class)
        .register(RaftAtomicCounterMapOperations.class)
        .register(RaftConsistentMapEvents.class)
        .register(RaftConsistentMapOperations.class)
        .register(RaftConsistentSetMultimapOperations.class)
        .register(RaftConsistentSetMultimapEvents.class)
        .register(RaftConsistentTreeMapOperations.class)
        .register(RaftAtomicCounterOperations.class)
        .register(RaftDocumentTreeEvents.class)
        .register(RaftDocumentTreeOperations.class)
        .register(RaftLeaderElectorEvents.class)
        .register(RaftLeaderElectorOperations.class)
        .register(RaftWorkQueueEvents.class)
        .register(RaftWorkQueueOperations.class)
        .register(RaftAtomicValueEvents.class)
        .register(RaftAtomicValueOperations.class)
        .register(RaftDistributedLockEvents.class)
        .register(RaftDistributedLockOperations.class)
        .build("RaftProtocol");

    /**
     * Raft storage namespace.
     */
    public static final KryoNamespace RAFT_STORAGE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
        .register(CloseSessionEntry.class)
        .register(CommandEntry.class)
        .register(ConfigurationEntry.class)
        .register(InitializeEntry.class)
        .register(KeepAliveEntry.class)
        .register(MetadataEntry.class)
        .register(OpenSessionEntry.class)
        .register(QueryEntry.class)
        .register(RaftOperation.class)
        .register(ReadConsistency.class)
        .register(ArrayList.class)
        .register(HashSet.class)
        .register(DefaultRaftMember.class)
        .register(MemberId.class)
        .register(RaftMember.Type.class)
        .register(Instant.class)
        .register(Configuration.class)
        .register(RaftAtomicCounterMapOperations.class)
        .register(RaftConsistentMapOperations.class)
        .register(RaftConsistentSetMultimapOperations.class)
        .register(RaftConsistentTreeMapOperations.class)
        .register(RaftAtomicCounterOperations.class)
        .register(RaftDocumentTreeOperations.class)
        .register(RaftLeaderElectorOperations.class)
        .register(RaftWorkQueueOperations.class)
        .register(RaftAtomicValueOperations.class)
        .register(RaftDistributedLockOperations.class)
        .build("RaftStorage");

    private RaftNamespaces() {
    }
}

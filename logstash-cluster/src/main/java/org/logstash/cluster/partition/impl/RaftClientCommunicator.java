package org.logstash.cluster.partition.impl;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.messaging.MessageSubject;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.CommandRequest;
import org.logstash.cluster.protocols.raft.protocol.CommandResponse;
import org.logstash.cluster.protocols.raft.protocol.HeartbeatRequest;
import org.logstash.cluster.protocols.raft.protocol.HeartbeatResponse;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveRequest;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveResponse;
import org.logstash.cluster.protocols.raft.protocol.MetadataRequest;
import org.logstash.cluster.protocols.raft.protocol.MetadataResponse;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.PublishRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.protocol.ResetRequest;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.serializer.Serializer;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class RaftClientCommunicator implements RaftClientProtocol {
    private final RaftMessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public RaftClientCommunicator(Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this(null, serializer, clusterCommunicator);
    }

    public RaftClientCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = new RaftMessageContext(prefix);
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    }

    private <T, U> CompletableFuture<U> sendAndReceive(MessageSubject subject, T request, MemberId memberId) {
        return clusterCommunicator.sendAndReceive(subject, request, serializer::encode, serializer::decode, NodeId.from(memberId.id()));
    }

    @Override
    public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
        return sendAndReceive(context.openSessionSubject, request, memberId);
    }

    @Override
    public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
        return sendAndReceive(context.closeSessionSubject, request, memberId);
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
        return sendAndReceive(context.keepAliveSubject, request, memberId);
    }

    @Override
    public CompletableFuture<QueryResponse> query(MemberId memberId, QueryRequest request) {
        return sendAndReceive(context.querySubject, request, memberId);
    }

    @Override
    public CompletableFuture<CommandResponse> command(MemberId memberId, CommandRequest request) {
        return sendAndReceive(context.commandSubject, request, memberId);
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
        return sendAndReceive(context.metadataSubject, request, memberId);
    }

    @Override
    public void registerHeartbeatHandler(Function<HeartbeatRequest, CompletableFuture<HeartbeatResponse>> handler) {
        clusterCommunicator.addSubscriber(context.heartbeatSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterHeartbeatHandler() {
        clusterCommunicator.removeSubscriber(context.heartbeatSubject);
    }

    @Override
    public void reset(Collection<MemberId> members, ResetRequest request) {
        Set<NodeId> nodes = members.stream().map(m -> NodeId.from(m.id())).collect(Collectors.toSet());
        clusterCommunicator.multicast(context.resetSubject(request.session()), request, serializer::encode, nodes);
    }

    @Override
    public void registerPublishListener(SessionId sessionId, Consumer<PublishRequest> listener, Executor executor) {
        clusterCommunicator.addSubscriber(context.publishSubject(sessionId.id()), serializer::decode, listener, executor);
    }

    @Override
    public void unregisterPublishListener(SessionId sessionId) {
        clusterCommunicator.removeSubscriber(context.publishSubject(sessionId.id()));
    }
}

package org.logstash.cluster.partition.impl;

import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.messaging.MessageSubject;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
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
import org.logstash.cluster.protocols.raft.protocol.RaftServerProtocol;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureRequest;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureResponse;
import org.logstash.cluster.protocols.raft.protocol.ResetRequest;
import org.logstash.cluster.protocols.raft.protocol.TransferRequest;
import org.logstash.cluster.protocols.raft.protocol.TransferResponse;
import org.logstash.cluster.protocols.raft.protocol.VoteRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteResponse;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.serializer.Serializer;

/**
 * Raft server protocol that uses a {@link ClusterCommunicationService}.
 */
public class RaftServerCommunicator implements RaftServerProtocol {
    private final RaftMessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public RaftServerCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = new RaftMessageContext(prefix);
        this.serializer = Preconditions.checkNotNull(serializer, "serializer cannot be null");
        this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
    }

    @Override
    public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
        return sendAndReceive(context.openSessionSubject, request, memberId);
    }

    private <T, U> CompletableFuture<U> sendAndReceive(MessageSubject subject, T request, MemberId memberId) {
        return clusterCommunicator.sendAndReceive(subject, request, serializer::encode, serializer::decode, NodeId.from(memberId.id()));
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
    public CompletableFuture<JoinResponse> join(MemberId memberId, JoinRequest request) {
        return sendAndReceive(context.joinSubject, request, memberId);
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(MemberId memberId, LeaveRequest request) {
        return sendAndReceive(context.leaveSubject, request, memberId);
    }

    @Override
    public CompletableFuture<ConfigureResponse> configure(MemberId memberId, ConfigureRequest request) {
        return sendAndReceive(context.configureSubject, request, memberId);
    }

    @Override
    public CompletableFuture<ReconfigureResponse> reconfigure(MemberId memberId, ReconfigureRequest request) {
        return sendAndReceive(context.reconfigureSubject, request, memberId);
    }

    @Override
    public CompletableFuture<InstallResponse> install(MemberId memberId, InstallRequest request) {
        return sendAndReceive(context.installSubject, request, memberId);
    }

    @Override
    public CompletableFuture<TransferResponse> transfer(MemberId memberId, TransferRequest request) {
        return sendAndReceive(context.transferSubject, request, memberId);
    }

    @Override
    public CompletableFuture<PollResponse> poll(MemberId memberId, PollRequest request) {
        return sendAndReceive(context.pollSubject, request, memberId);
    }

    @Override
    public CompletableFuture<VoteResponse> vote(MemberId memberId, VoteRequest request) {
        return sendAndReceive(context.voteSubject, request, memberId);
    }

    @Override
    public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
        return sendAndReceive(context.appendSubject, request, memberId);
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(MemberId memberId, HeartbeatRequest request) {
        return sendAndReceive(context.heartbeatSubject, request, memberId);
    }

    @Override
    public void publish(MemberId memberId, PublishRequest request) {
        clusterCommunicator.unicast(context.publishSubject(request.session()), request, serializer::encode, NodeId.from(memberId.id()));
    }

    @Override
    public void registerOpenSessionHandler(Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> handler) {
        clusterCommunicator.addSubscriber(context.openSessionSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterOpenSessionHandler() {
        clusterCommunicator.removeSubscriber(context.openSessionSubject);
    }

    @Override
    public void registerCloseSessionHandler(Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> handler) {
        clusterCommunicator.addSubscriber(context.closeSessionSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterCloseSessionHandler() {
        clusterCommunicator.removeSubscriber(context.closeSessionSubject);
    }

    @Override
    public void registerKeepAliveHandler(Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> handler) {
        clusterCommunicator.addSubscriber(context.keepAliveSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterKeepAliveHandler() {
        clusterCommunicator.removeSubscriber(context.keepAliveSubject);
    }

    @Override
    public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
        clusterCommunicator.addSubscriber(context.querySubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterQueryHandler() {
        clusterCommunicator.removeSubscriber(context.querySubject);
    }

    @Override
    public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
        clusterCommunicator.addSubscriber(context.commandSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterCommandHandler() {
        clusterCommunicator.removeSubscriber(context.commandSubject);
    }

    @Override
    public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
        clusterCommunicator.addSubscriber(context.metadataSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterMetadataHandler() {
        clusterCommunicator.removeSubscriber(context.metadataSubject);
    }

    @Override
    public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
        clusterCommunicator.addSubscriber(context.joinSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterJoinHandler() {
        clusterCommunicator.removeSubscriber(context.joinSubject);
    }

    @Override
    public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
        clusterCommunicator.addSubscriber(context.leaveSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterLeaveHandler() {
        clusterCommunicator.removeSubscriber(context.leaveSubject);
    }

    @Override
    public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
        clusterCommunicator.addSubscriber(context.transferSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterTransferHandler() {
        clusterCommunicator.removeSubscriber(context.transferSubject);
    }

    @Override
    public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
        clusterCommunicator.addSubscriber(context.configureSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterConfigureHandler() {
        clusterCommunicator.removeSubscriber(context.configureSubject);
    }

    @Override
    public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
        clusterCommunicator.addSubscriber(context.reconfigureSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterReconfigureHandler() {
        clusterCommunicator.removeSubscriber(context.reconfigureSubject);
    }

    @Override
    public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
        clusterCommunicator.addSubscriber(context.installSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterInstallHandler() {
        clusterCommunicator.removeSubscriber(context.installSubject);
    }

    @Override
    public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
        clusterCommunicator.addSubscriber(context.pollSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterPollHandler() {
        clusterCommunicator.removeSubscriber(context.pollSubject);
    }

    @Override
    public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
        clusterCommunicator.addSubscriber(context.voteSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterVoteHandler() {
        clusterCommunicator.removeSubscriber(context.voteSubject);
    }

    @Override
    public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
        clusterCommunicator.addSubscriber(context.appendSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterAppendHandler() {
        clusterCommunicator.removeSubscriber(context.appendSubject);
    }

    @Override
    public void registerResetListener(SessionId sessionId, Consumer<ResetRequest> listener, Executor executor) {
        clusterCommunicator.addSubscriber(context.resetSubject(sessionId.id()), serializer::decode, listener, executor);
    }

    @Override
    public void unregisterResetListener(SessionId sessionId) {
        clusterCommunicator.removeSubscriber(context.resetSubject(sessionId.id()));
    }
}

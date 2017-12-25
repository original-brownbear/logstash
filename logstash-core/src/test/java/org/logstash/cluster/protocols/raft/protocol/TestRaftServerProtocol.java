package org.logstash.cluster.protocols.raft.protocol;

import com.google.common.collect.Maps;
import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * Test server protocol.
 */
public class TestRaftServerProtocol extends TestRaftProtocol implements RaftServerProtocol {
    private final Map<Long, Consumer<ResetRequest>> resetListeners = Maps.newConcurrentMap();
    private Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> openSessionHandler;
    private Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> closeSessionHandler;
    private Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> keepAliveHandler;
    private Function<QueryRequest, CompletableFuture<QueryResponse>> queryHandler;
    private Function<CommandRequest, CompletableFuture<CommandResponse>> commandHandler;
    private Function<MetadataRequest, CompletableFuture<MetadataResponse>> metadataHandler;
    private Function<JoinRequest, CompletableFuture<JoinResponse>> joinHandler;
    private Function<LeaveRequest, CompletableFuture<LeaveResponse>> leaveHandler;
    private Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> configureHandler;
    private Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> reconfigureHandler;
    private Function<InstallRequest, CompletableFuture<InstallResponse>> installHandler;
    private Function<TransferRequest, CompletableFuture<TransferResponse>> transferHandler;
    private Function<PollRequest, CompletableFuture<PollResponse>> pollHandler;
    private Function<VoteRequest, CompletableFuture<VoteResponse>> voteHandler;
    private Function<AppendRequest, CompletableFuture<AppendResponse>> appendHandler;

    public TestRaftServerProtocol(MemberId memberId, Map<MemberId, TestRaftServerProtocol> servers, Map<MemberId, TestRaftClientProtocol> clients) {
        super(servers, clients);
        servers.put(memberId, this);
    }

    @Override
    public CompletableFuture<OpenSessionResponse> openSession(MemberId memberId, OpenSessionRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.openSession(request));
    }

    private CompletableFuture<TestRaftServerProtocol> getServer(MemberId memberId) {
        TestRaftServerProtocol server = server(memberId);
        if (server != null) {
            return Futures.completedFuture(server);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    @Override
    public CompletableFuture<CloseSessionResponse> closeSession(MemberId memberId, CloseSessionRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.closeSession(request));
    }

    @Override
    public CompletableFuture<KeepAliveResponse> keepAlive(MemberId memberId, KeepAliveRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.keepAlive(request));
    }

    @Override
    public CompletableFuture<QueryResponse> query(MemberId memberId, QueryRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.query(request));
    }

    @Override
    public CompletableFuture<CommandResponse> command(MemberId memberId, CommandRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.command(request));
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MemberId memberId, MetadataRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.metadata(request));
    }

    @Override
    public CompletableFuture<JoinResponse> join(MemberId memberId, JoinRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.join(request));
    }

    @Override
    public CompletableFuture<LeaveResponse> leave(MemberId memberId, LeaveRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.leave(request));
    }

    @Override
    public CompletableFuture<ConfigureResponse> configure(MemberId memberId, ConfigureRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.configure(request));
    }

    @Override
    public CompletableFuture<ReconfigureResponse> reconfigure(MemberId memberId, ReconfigureRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.reconfigure(request));
    }

    @Override
    public CompletableFuture<InstallResponse> install(MemberId memberId, InstallRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.install(request));
    }

    @Override
    public CompletableFuture<TransferResponse> transfer(MemberId memberId, TransferRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.transfer(request));
    }

    @Override
    public CompletableFuture<PollResponse> poll(MemberId memberId, PollRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.poll(request));
    }

    @Override
    public CompletableFuture<VoteResponse> vote(MemberId memberId, VoteRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.vote(request));
    }

    @Override
    public CompletableFuture<AppendResponse> append(MemberId memberId, AppendRequest request) {
        return getServer(memberId).thenCompose(listener -> listener.append(request));
    }

    @Override
    public CompletableFuture<HeartbeatResponse> heartbeat(MemberId memberId, HeartbeatRequest request) {
        return getClient(memberId).thenCompose(protocol -> protocol.heartbeat(request));
    }

    @Override
    public void publish(MemberId memberId, PublishRequest request) {
        getClient(memberId).thenAccept(protocol -> protocol.publish(request));
    }

    private CompletableFuture<TestRaftClientProtocol> getClient(MemberId memberId) {
        TestRaftClientProtocol client = client(memberId);
        if (client != null) {
            return Futures.completedFuture(client);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    @Override
    public void registerOpenSessionHandler(Function<OpenSessionRequest, CompletableFuture<OpenSessionResponse>> handler) {
        this.openSessionHandler = handler;
    }

    @Override
    public void unregisterOpenSessionHandler() {
        this.openSessionHandler = null;
    }

    @Override
    public void registerCloseSessionHandler(Function<CloseSessionRequest, CompletableFuture<CloseSessionResponse>> handler) {
        this.closeSessionHandler = handler;
    }

    @Override
    public void unregisterCloseSessionHandler() {
        this.closeSessionHandler = null;
    }

    @Override
    public void registerKeepAliveHandler(Function<KeepAliveRequest, CompletableFuture<KeepAliveResponse>> handler) {
        this.keepAliveHandler = handler;
    }

    @Override
    public void unregisterKeepAliveHandler() {
        this.keepAliveHandler = null;
    }

    @Override
    public void registerQueryHandler(Function<QueryRequest, CompletableFuture<QueryResponse>> handler) {
        this.queryHandler = handler;
    }

    @Override
    public void unregisterQueryHandler() {
        this.queryHandler = null;
    }

    @Override
    public void registerCommandHandler(Function<CommandRequest, CompletableFuture<CommandResponse>> handler) {
        this.commandHandler = handler;
    }

    @Override
    public void unregisterCommandHandler() {
        this.commandHandler = null;
    }

    @Override
    public void registerMetadataHandler(Function<MetadataRequest, CompletableFuture<MetadataResponse>> handler) {
        this.metadataHandler = handler;
    }

    @Override
    public void unregisterMetadataHandler() {
        this.metadataHandler = null;
    }

    @Override
    public void registerJoinHandler(Function<JoinRequest, CompletableFuture<JoinResponse>> handler) {
        this.joinHandler = handler;
    }

    @Override
    public void unregisterJoinHandler() {
        this.joinHandler = null;
    }

    @Override
    public void registerLeaveHandler(Function<LeaveRequest, CompletableFuture<LeaveResponse>> handler) {
        this.leaveHandler = handler;
    }

    @Override
    public void unregisterLeaveHandler() {
        this.leaveHandler = null;
    }

    @Override
    public void registerTransferHandler(Function<TransferRequest, CompletableFuture<TransferResponse>> handler) {
        this.transferHandler = handler;
    }

    @Override
    public void unregisterTransferHandler() {
        this.transferHandler = null;
    }

    @Override
    public void registerConfigureHandler(Function<ConfigureRequest, CompletableFuture<ConfigureResponse>> handler) {
        this.configureHandler = handler;
    }

    @Override
    public void unregisterConfigureHandler() {
        this.configureHandler = null;
    }

    @Override
    public void registerReconfigureHandler(Function<ReconfigureRequest, CompletableFuture<ReconfigureResponse>> handler) {
        this.reconfigureHandler = handler;
    }

    @Override
    public void unregisterReconfigureHandler() {
        this.reconfigureHandler = null;
    }

    @Override
    public void registerInstallHandler(Function<InstallRequest, CompletableFuture<InstallResponse>> handler) {
        this.installHandler = handler;
    }

    @Override
    public void unregisterInstallHandler() {
        this.installHandler = null;
    }

    @Override
    public void registerPollHandler(Function<PollRequest, CompletableFuture<PollResponse>> handler) {
        this.pollHandler = handler;
    }

    @Override
    public void unregisterPollHandler() {
        this.pollHandler = null;
    }

    @Override
    public void registerVoteHandler(Function<VoteRequest, CompletableFuture<VoteResponse>> handler) {
        this.voteHandler = handler;
    }

    @Override
    public void unregisterVoteHandler() {
        this.voteHandler = null;
    }

    @Override
    public void registerAppendHandler(Function<AppendRequest, CompletableFuture<AppendResponse>> handler) {
        this.appendHandler = handler;
    }

    @Override
    public void unregisterAppendHandler() {
        this.appendHandler = null;
    }

    @Override
    public void registerResetListener(SessionId sessionId, Consumer<ResetRequest> listener, Executor executor) {
        resetListeners.put(sessionId.id(), request -> executor.execute(() -> listener.accept(request)));
    }

    @Override
    public void unregisterResetListener(SessionId sessionId) {
        resetListeners.remove(sessionId.id());
    }

    CompletableFuture<AppendResponse> append(AppendRequest request) {
        if (appendHandler != null) {
            return appendHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<VoteResponse> vote(VoteRequest request) {
        if (voteHandler != null) {
            return voteHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<PollResponse> poll(PollRequest request) {
        if (pollHandler != null) {
            return pollHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<TransferResponse> transfer(TransferRequest request) {
        if (transferHandler != null) {
            return transferHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<InstallResponse> install(InstallRequest request) {
        if (installHandler != null) {
            return installHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
        if (reconfigureHandler != null) {
            return reconfigureHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
        if (configureHandler != null) {
            return configureHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
        if (leaveHandler != null) {
            return leaveHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<JoinResponse> join(JoinRequest request) {
        if (joinHandler != null) {
            return joinHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        if (metadataHandler != null) {
            return metadataHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<CommandResponse> command(CommandRequest request) {
        if (commandHandler != null) {
            return commandHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<QueryResponse> query(QueryRequest request) {
        if (queryHandler != null) {
            return queryHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
        if (keepAliveHandler != null) {
            return keepAliveHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
        if (closeSessionHandler != null) {
            return closeSessionHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
        if (openSessionHandler != null) {
            return openSessionHandler.apply(request);
        } else {
            return Futures.exceptionalFuture(new ConnectException());
        }
    }

    void reset(ResetRequest request) {
        Consumer<ResetRequest> listener = resetListeners.get(request.session());
        if (listener != null) {
            listener.accept(request);
        }
    }
}

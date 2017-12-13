package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.ThreadModel;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;
import org.logstash.cluster.protocols.raft.cluster.impl.DefaultRaftMember;
import org.logstash.cluster.protocols.raft.cluster.impl.RaftClusterContext;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftServerProtocol;
import org.logstash.cluster.protocols.raft.protocol.TransferRequest;
import org.logstash.cluster.protocols.raft.roles.ActiveRole;
import org.logstash.cluster.protocols.raft.roles.CandidateRole;
import org.logstash.cluster.protocols.raft.roles.FollowerRole;
import org.logstash.cluster.protocols.raft.roles.InactiveRole;
import org.logstash.cluster.protocols.raft.roles.LeaderRole;
import org.logstash.cluster.protocols.raft.roles.PassiveRole;
import org.logstash.cluster.protocols.raft.roles.PromotableRole;
import org.logstash.cluster.protocols.raft.roles.RaftRole;
import org.logstash.cluster.protocols.raft.session.impl.RaftSessionRegistry;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.protocols.raft.storage.compactor.RaftLogCompactor;
import org.logstash.cluster.protocols.raft.storage.log.RaftLog;
import org.logstash.cluster.protocols.raft.storage.log.RaftLogReader;
import org.logstash.cluster.protocols.raft.storage.log.RaftLogWriter;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotStore;
import org.logstash.cluster.protocols.raft.storage.system.MetaStore;
import org.logstash.cluster.protocols.raft.utils.LoadMonitor;
import org.logstash.cluster.utils.concurrent.SingleThreadContext;
import org.logstash.cluster.utils.concurrent.ThreadContext;
import org.logstash.cluster.utils.concurrent.ThreadContextFactory;
import org.logstash.cluster.utils.concurrent.Threads;

/**
 * Manages the volatile state and state transitions of a Raft server.
 * <p>
 * This class is the primary vehicle for managing the state of a server. All state that is shared across roles (i.e. follower, candidate, leader)
 * is stored in the cluster state. This includes Raft-specific state like the current leader and term, the log, and the cluster configuration.
 */
public class RaftContext implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(RaftContext.class);

    private static final int LOAD_WINDOW_SIZE = 5;
    private static final int HIGH_LOAD_THRESHOLD = 500;
    protected final String name;
    protected final ThreadContext threadContext;
    protected final RaftServiceFactoryRegistry serviceFactories;
    protected final RaftClusterContext cluster;
    protected final RaftServerProtocol protocol;
    protected final RaftStorage storage;
    protected final RaftServiceRegistry services = new RaftServiceRegistry();
    protected final RaftSessionRegistry sessions = new RaftSessionRegistry();
    private final Set<Consumer<RaftServer.Role>> roleChangeListeners = new CopyOnWriteArraySet<>();
    private final Set<Consumer<RaftContext.State>> stateChangeListeners = new CopyOnWriteArraySet<>();
    private final Set<Consumer<RaftMember>> electionListeners = new CopyOnWriteArraySet<>();
    private final LoadMonitor loadMonitor;
    private final MetaStore meta;
    private final RaftLog raftLog;
    private final RaftLogWriter logWriter;
    private final RaftLogReader logReader;
    private final RaftLogCompactor logCompactor;
    private final SnapshotStore snapshotStore;
    private final RaftServiceManager stateMachine;
    private final ThreadContextFactory threadContextFactory;
    private final ThreadContext loadContext;
    private final ThreadContext compactionContext;
    protected RaftRole role = new InactiveRole(this);
    private volatile RaftContext.State state = RaftContext.State.ACTIVE;
    private Duration electionTimeout = Duration.ofMillis(500);
    private Duration heartbeatInterval = Duration.ofMillis(150);
    private int electionThreshold = 3;
    private Duration sessionTimeout = Duration.ofMillis(5000);
    private int sessionFailureThreshold = 5;
    private volatile MemberId leader;
    private volatile long term;
    private MemberId lastVotedFor;
    private long lastHeartbeatTime;
    private long commitIndex;
    private volatile long firstCommitIndex;
    private volatile long lastApplied;

    @SuppressWarnings("unchecked")
    public RaftContext(
        final String name,
        final MemberId localMemberId,
        final RaftServerProtocol protocol,
        final RaftStorage storage,
        final RaftServiceFactoryRegistry serviceFactories,
        final ThreadModel threadModel,
        final int threadPoolSize) {
        this.name = Preconditions.checkNotNull(name, "name cannot be null");
        this.protocol = Preconditions.checkNotNull(protocol, "protocol cannot be null");
        this.storage = Preconditions.checkNotNull(storage, "storage cannot be null");
        this.serviceFactories = Preconditions.checkNotNull(serviceFactories, "registry cannot be null");

        final String baseThreadName = String.format("raft-server-%s", name);
        this.threadContext = new SingleThreadContext(Threads.namedThreads(baseThreadName, LOGGER));
        this.loadContext = new SingleThreadContext(Threads.namedThreads(baseThreadName + "-load", LOGGER));
        this.compactionContext = new SingleThreadContext(Threads.namedThreads(baseThreadName + "-compaction", LOGGER));

        this.threadContextFactory = threadModel.factory(baseThreadName + "-%d", threadPoolSize, LOGGER);

        this.loadMonitor = new LoadMonitor(LOAD_WINDOW_SIZE, HIGH_LOAD_THRESHOLD, loadContext);

        // Open the metadata store.
        this.meta = storage.openMetaStore();

        // Load the current term and last vote from disk.
        this.term = meta.loadTerm();
        this.lastVotedFor = meta.loadVote();

        // Construct the core log, reader, writer, and compactor.
        this.raftLog = storage.openLog();
        this.logWriter = raftLog.writer();
        this.logReader = raftLog.openReader(1, RaftLogReader.Mode.ALL);
        this.logCompactor = new RaftLogCompactor(this, compactionContext);

        // Open the snapshot store.
        this.snapshotStore = storage.openSnapshotStore();

        // Create a new internal server state machine.
        this.stateMachine = new RaftServiceManager(this, threadContextFactory);

        this.cluster = new RaftClusterContext(localMemberId, this);

        // Register protocol listeners.
        registerHandlers(protocol);
    }

    /**
     * Registers server handlers on the configured protocol.
     */
    private void registerHandlers(final RaftServerProtocol protocol) {
        protocol.registerOpenSessionHandler(request -> runOnContext(() -> role.onOpenSession(request)));
        protocol.registerCloseSessionHandler(request -> runOnContext(() -> role.onCloseSession(request)));
        protocol.registerKeepAliveHandler(request -> runOnContext(() -> role.onKeepAlive(request)));
        protocol.registerMetadataHandler(request -> runOnContext(() -> role.onMetadata(request)));
        protocol.registerConfigureHandler(request -> runOnContext(() -> role.onConfigure(request)));
        protocol.registerInstallHandler(request -> runOnContext(() -> role.onInstall(request)));
        protocol.registerJoinHandler(request -> runOnContext(() -> role.onJoin(request)));
        protocol.registerReconfigureHandler(request -> runOnContext(() -> role.onReconfigure(request)));
        protocol.registerLeaveHandler(request -> runOnContext(() -> role.onLeave(request)));
        protocol.registerTransferHandler(request -> runOnContext(() -> role.onTransfer(request)));
        protocol.registerAppendHandler(request -> runOnContext(() -> role.onAppend(request)));
        protocol.registerPollHandler(request -> runOnContext(() -> role.onPoll(request)));
        protocol.registerVoteHandler(request -> runOnContext(() -> role.onVote(request)));
        protocol.registerCommandHandler(request -> runOnContext(() -> role.onCommand(request)));
        protocol.registerQueryHandler(request -> runOnContext(() -> role.onQuery(request)));
    }

    private <R extends RaftResponse> CompletableFuture<R> runOnContext(final Supplier<CompletableFuture<R>> function) {
        final CompletableFuture<R> future = new CompletableFuture<>();
        threadContext.execute(() -> function.get().whenComplete((response, error) -> {
            if (error == null) {
                future.complete(response);
            } else {
                future.completeExceptionally(error);
            }
        }));
        return future;
    }

    /**
     * Returns the server name.
     * @return The server name.
     */
    public String getName() {
        return name;
    }

    /**
     * Adds a role change listener.
     * @param listener The role change listener.
     */
    public void addRoleChangeListener(final Consumer<RaftServer.Role> listener) {
        roleChangeListeners.add(listener);
    }

    /**
     * Removes a role change listener.
     * @param listener The role change listener.
     */
    public void removeRoleChangeListener(final Consumer<RaftServer.Role> listener) {
        roleChangeListeners.remove(listener);
    }

    /**
     * Awaits a state change.
     * @param state the state for which to wait
     * @param listener the listener to call when the next state change occurs
     */
    public void awaitState(final RaftContext.State state, final Consumer<RaftContext.State> listener) {
        if (this.state == state) {
            listener.accept(this.state);
        } else {
            addStateChangeListener(new Consumer<RaftContext.State>() {
                @Override
                public void accept(final RaftContext.State state) {
                    listener.accept(state);
                    removeStateChangeListener(this);
                }
            });
        }
    }

    /**
     * Adds a state change listener.
     * @param listener The state change listener.
     */
    public void addStateChangeListener(final Consumer<RaftContext.State> listener) {
        stateChangeListeners.add(listener);
    }

    /**
     * Removes a state change listener.
     * @param listener The state change listener.
     */
    public void removeStateChangeListener(final Consumer<RaftContext.State> listener) {
        stateChangeListeners.remove(listener);
    }

    /**
     * Returns the execution context.
     * @return The execution context.
     */
    public ThreadContext getThreadContext() {
        return threadContext;
    }

    /**
     * Returns the server protocol.
     * @return The server protocol.
     */
    public RaftServerProtocol getProtocol() {
        return protocol;
    }

    /**
     * Returns the server storage.
     * @return The server storage.
     */
    public RaftStorage getStorage() {
        return storage;
    }

    /**
     * Returns the current server state.
     * @return the current server state
     */
    public RaftContext.State getState() {
        return state;
    }

    /**
     * Returns the election timeout.
     * @return The election timeout.
     */
    public Duration getElectionTimeout() {
        return electionTimeout;
    }

    /**
     * Sets the election timeout.
     * @param electionTimeout The election timeout.
     */
    public void setElectionTimeout(final Duration electionTimeout) {
        this.electionTimeout = electionTimeout;
    }

    /**
     * Returns the heartbeat interval.
     * @return The heartbeat interval.
     */
    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets the heartbeat interval.
     * @param heartbeatInterval The Raft heartbeat interval.
     */
    public void setHeartbeatInterval(final Duration heartbeatInterval) {
        this.heartbeatInterval = Preconditions.checkNotNull(heartbeatInterval, "heartbeatInterval cannot be null");
    }

    /**
     * Returns the election threshold.
     * @return the election threshold
     */
    public int getElectionThreshold() {
        return electionThreshold;
    }

    /**
     * Sets the election threshold.
     * @param electionThreshold the election threshold
     */
    public void setElectionThreshold(final int electionThreshold) {
        this.electionThreshold = electionThreshold;
    }

    /**
     * Returns the session timeout.
     * @return The session timeout.
     */
    public Duration getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets the session timeout.
     * @param sessionTimeout The session timeout.
     */
    public void setSessionTimeout(final Duration sessionTimeout) {
        this.sessionTimeout = Preconditions.checkNotNull(sessionTimeout, "sessionTimeout cannot be null");
    }

    /**
     * Returns the session failure threshold.
     * @return the session failure threshold
     */
    public int getSessionFailureThreshold() {
        return sessionFailureThreshold;
    }

    /**
     * Sets the session failure threshold.
     * @param sessionFailureThreshold the session failure threshold
     */
    public void setSessionFailureThreshold(final int sessionFailureThreshold) {
        this.sessionFailureThreshold = sessionFailureThreshold;
    }

    /**
     * Returns a boolean indicating whether this server is the current leader.
     * @return Indicates whether this server is the leader.
     */
    public boolean isLeader() {
        final MemberId leader = this.leader;
        return leader != null && leader.equals(cluster.getMember().memberId());
    }

    /**
     * Sets the state leader.
     * @param leader The state leader.
     */
    public void setLeader(final MemberId leader) {
        if (!Objects.equals(this.leader, leader)) {
            if (leader == null) {
                this.leader = null;
            } else {
                // If a valid leader ID was specified, it must be a member that's currently a member of the
                // ACTIVE members configuration. Note that we don't throw exceptions for unknown members. It's
                // possible that a failure following a configuration change could result in an unknown leader
                // sending AppendRequest to this server. Simply configure the leader if it's known.
                final DefaultRaftMember member = cluster.getMember(leader);
                if (member != null) {
                    this.leader = leader;
                    LOGGER.info("Found leader {}", member.memberId());
                    electionListeners.forEach(l -> l.accept(member));
                }
            }

            this.lastVotedFor = null;
            meta.storeVote(null);
        }
    }

    /**
     * Returns the state term.
     * @return The state term.
     */
    public long getTerm() {
        return term;
    }

    /**
     * Sets the state term.
     * @param term The state term.
     */
    public void setTerm(final long term) {
        if (term > this.term) {
            this.term = term;
            this.leader = null;
            this.lastVotedFor = null;
            meta.storeTerm(this.term);
            meta.storeVote(this.lastVotedFor);
            LOGGER.debug("Set term {}", term);
        }
    }

    /**
     * Returns the last time a request was received from the leader.
     * @return The last time a request was received
     */
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime;
    }

    /**
     * Sets the last time a request was received by the node.
     * @param lastHeartbeatTime The last time a request was received
     */
    public void setLastHeartbeatTime(final long lastHeartbeatTime) {
        this.lastHeartbeatTime = lastHeartbeatTime;
    }

    /**
     * Sets the last time a request was received from the leader.
     */
    public void setLastHeartbeatTime() {
        setLastHeartbeatTime(System.currentTimeMillis());
    }

    /**
     * Returns the state last voted for candidate.
     * @return The state last voted for candidate.
     */
    public MemberId getLastVotedFor() {
        return lastVotedFor;
    }

    /**
     * Sets the state last voted for candidate.
     * @param candidate The candidate that was voted for.
     */
    public void setLastVotedFor(final MemberId candidate) {
        // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
        Preconditions.checkState(!(lastVotedFor != null && candidate != null), "Already voted for another candidate");
        final DefaultRaftMember member = cluster.getMember(candidate);
        Preconditions.checkState(member != null, "Unknown candidate: %d", candidate);
        this.lastVotedFor = candidate;
        meta.storeVote(this.lastVotedFor);

        if (candidate != null) {
            LOGGER.debug("Voted for {}", member.memberId());
        } else {
            LOGGER.trace("Reset last voted for");
        }
    }

    /**
     * Sets the commit index.
     * @param commitIndex The commit index.
     * @return the previous commit index
     */
    public long setCommitIndex(final long commitIndex) {
        Preconditions.checkArgument(commitIndex >= 0, "commitIndex must be positive");
        final long previousCommitIndex = this.commitIndex;
        if (commitIndex > previousCommitIndex) {
            this.commitIndex = commitIndex;
            logWriter.commit(Math.min(commitIndex, logWriter.getLastIndex()));
            final long configurationIndex = cluster.getConfiguration().index();
            if (configurationIndex > previousCommitIndex && configurationIndex <= commitIndex) {
                cluster.commit();
            }
            setFirstCommitIndex(commitIndex);
        }
        return previousCommitIndex;
    }

    /**
     * Returns the commit index.
     * @return The commit index.
     */
    public long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Returns the first commit index.
     * @return The first commit index.
     */
    public long getFirstCommitIndex() {
        return firstCommitIndex;
    }

    /**
     * Sets the first commit index.
     * @param firstCommitIndex The first commit index.
     */
    public void setFirstCommitIndex(final long firstCommitIndex) {
        if (this.firstCommitIndex == 0) {
            this.firstCommitIndex = firstCommitIndex;
        }
    }

    /**
     * Returns the last applied index.
     * @return the last applied index
     */
    public long getLastApplied() {
        return lastApplied;
    }

    /**
     * Sets the last applied index.
     * @param lastApplied the last applied index
     */
    public void setLastApplied(final long lastApplied) {
        this.lastApplied = Math.max(this.lastApplied, lastApplied);
        if (state == RaftContext.State.ACTIVE) {
            threadContext.execute(() -> {
                if (state == RaftContext.State.ACTIVE && this.lastApplied >= firstCommitIndex) {
                    state = RaftContext.State.READY;
                    stateChangeListeners.forEach(l -> l.accept(state));
                }
            });
        }
    }

    /**
     * Returns the server load monitor.
     * @return the server load monitor
     */
    public LoadMonitor getLoadMonitor() {
        return loadMonitor;
    }

    /**
     * Returns the server state machine.
     * @return The server state machine.
     */
    public RaftServiceManager getStateMachine() {
        return stateMachine;
    }

    /**
     * Returns the server service registry.
     * @return the server service registry
     */
    public RaftServiceRegistry getServices() {
        return services;
    }

    /**
     * Returns the server session registry.
     * @return the server session registry
     */
    public RaftSessionRegistry getSessions() {
        return sessions;
    }

    /**
     * Returns the server state machine registry.
     * @return The server state machine registry.
     */
    public RaftServiceFactoryRegistry getServiceFactories() {
        return serviceFactories;
    }

    /**
     * Returns the current server role.
     * @return The current server role.
     */
    public RaftServer.Role getRole() {
        return role.role();
    }

    /**
     * Returns the current server state.
     * @return The current server state.
     */
    public RaftRole getRaftRole() {
        return role;
    }

    /**
     * Returns the server metadata store.
     * @return The server metadata store.
     */
    public MetaStore getMetaStore() {
        return meta;
    }

    /**
     * Returns the server log.
     * @return The server log.
     */
    public RaftLog getLog() {
        return raftLog;
    }

    /**
     * Returns the server log writer.
     * @return The log writer.
     */
    public RaftLogWriter getLogWriter() {
        return logWriter;
    }

    /**
     * Returns the server log reader.
     * @return The log reader.
     */
    public RaftLogReader getLogReader() {
        return logReader;
    }

    /**
     * Returns the Raft log compactor.
     * @return the Raft log compactor
     */
    public RaftLogCompactor getLogCompactor() {
        return logCompactor;
    }

    /**
     * Returns the server snapshot store.
     * @return The server snapshot store.
     */
    public SnapshotStore getSnapshotStore() {
        return snapshotStore;
    }

    /**
     * Attempts to become the leader.
     */
    public CompletableFuture<Void> anoint() {
        if (role.role() == RaftServer.Role.LEADER) {
            return CompletableFuture.completedFuture(null);
        }

        final CompletableFuture<Void> future = new CompletableFuture<>();

        threadContext.execute(() -> {
            // Register a leader election listener to wait for the election of this node.
            final Consumer<RaftMember> electionListener = new Consumer<RaftMember>() {
                @Override
                public void accept(final RaftMember member) {
                    if (member.memberId().equals(cluster.getMember().memberId())) {
                        future.complete(null);
                    } else {
                        future.completeExceptionally(new RaftException.ProtocolException("Failed to transfer leadership"));
                    }
                    removeLeaderElectionListener(this);
                }
            };
            addLeaderElectionListener(electionListener);

            // If a leader already exists, request a leadership transfer from it. Otherwise, transition to the candidate
            // state and attempt to get elected.
            final RaftMember member = getCluster().getMember();
            final RaftMember leader = getLeader();
            if (leader != null) {
                protocol.transfer(leader.memberId(), TransferRequest.builder()
                    .withMember(member.memberId())
                    .build()).whenCompleteAsync((response, error) -> {
                    if (error != null) {
                        future.completeExceptionally(error);
                    } else if (response.status() == RaftResponse.Status.ERROR) {
                        future.completeExceptionally(response.error().createException());
                    } else {
                        transition(RaftServer.Role.CANDIDATE);
                    }
                }, threadContext);
            } else {
                transition(RaftServer.Role.CANDIDATE);
            }
        });
        return future;
    }

    /**
     * Adds a leader election listener.
     * @param listener The leader election listener.
     */
    public void addLeaderElectionListener(final Consumer<RaftMember> listener) {
        electionListeners.add(listener);
    }

    /**
     * Removes a leader election listener.
     * @param listener The leader election listener.
     */
    public void removeLeaderElectionListener(final Consumer<RaftMember> listener) {
        electionListeners.remove(listener);
    }

    /**
     * Returns the cluster state.
     * @return The cluster state.
     */
    public RaftClusterContext getCluster() {
        return cluster;
    }

    /**
     * Returns the state leader.
     * @return The state leader.
     */
    public DefaultRaftMember getLeader() {
        // Store in a local variable to prevent race conditions and/or multiple volatile lookups.
        final MemberId leader = this.leader;
        return leader != null ? cluster.getMember(leader) : null;
    }

    /**
     * Transition handler.
     */
    public void transition(final RaftServer.Role role) {
        checkThread();

        if (this.role != null && role == this.role.role()) {
            return;
        }

        LOGGER.info("Transitioning to {}", role);

        // Close the old state.
        try {
            this.role.close().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("failed to close Raft state", e);
        }

        // Force state transitions to occur synchronously in order to prevent race conditions.
        try {
            this.role = createRole(role);
            this.role.open().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("failed to initialize Raft state", e);
        }

        roleChangeListeners.forEach(l -> l.accept(this.role.role()));
    }

    /**
     * Checks that the current thread is the state context thread.
     */
    public void checkThread() {
        threadContext.checkThread();
    }

    /**
     * Creates an internal state for the given state type.
     */
    private RaftRole createRole(final RaftServer.Role role) {
        switch (role) {
            case INACTIVE:
                return new InactiveRole(this);
            case PASSIVE:
                return new PassiveRole(this);
            case PROMOTABLE:
                return new PromotableRole(this);
            case FOLLOWER:
                return new FollowerRole(this);
            case CANDIDATE:
                return new CandidateRole(this);
            case LEADER:
                return new LeaderRole(this);
            default:
                throw new AssertionError();
        }
    }

    /**
     * Transitions the server to the base state for the given member type.
     */
    public void transition(final RaftMember.Type type) {
        switch (type) {
            case ACTIVE:
                if (!(role instanceof ActiveRole)) {
                    transition(RaftServer.Role.FOLLOWER);
                }
                break;
            case PROMOTABLE:
                if (this.role.role() != RaftServer.Role.PROMOTABLE) {
                    transition(RaftServer.Role.PROMOTABLE);
                }
                break;
            case PASSIVE:
                if (this.role.role() != RaftServer.Role.PASSIVE) {
                    transition(RaftServer.Role.PASSIVE);
                }
                break;
            default:
                if (this.role.role() != RaftServer.Role.INACTIVE) {
                    transition(RaftServer.Role.INACTIVE);
                }
                break;
        }
    }

    @Override
    public void close() {
        // Unregister protocol listeners.
        unregisterHandlers(protocol);

        // Close the log.
        try {
            raftLog.close();
        } catch (final Exception e) {
        }

        // Close the metastore.
        try {
            meta.close();
        } catch (final Exception e) {
        }

        // Close the snapshot store.
        try {
            snapshotStore.close();
        } catch (final Exception e) {
        }

        // Close the state machine and thread context.
        stateMachine.close();
        threadContext.close();
        loadContext.close();
        compactionContext.close();
        threadContextFactory.close();
    }

    /**
     * Unregisters server handlers on the configured protocol.
     */
    private static void unregisterHandlers(final RaftServerProtocol protocol) {
        protocol.unregisterOpenSessionHandler();
        protocol.unregisterCloseSessionHandler();
        protocol.unregisterKeepAliveHandler();
        protocol.unregisterMetadataHandler();
        protocol.unregisterConfigureHandler();
        protocol.unregisterInstallHandler();
        protocol.unregisterJoinHandler();
        protocol.unregisterReconfigureHandler();
        protocol.unregisterLeaveHandler();
        protocol.unregisterTransferHandler();
        protocol.unregisterAppendHandler();
        protocol.unregisterPollHandler();
        protocol.unregisterVoteHandler();
        protocol.unregisterCommandHandler();
        protocol.unregisterQueryHandler();
    }

    /**
     * Deletes the server context.
     */
    public void delete() {
        // Delete the log.
        storage.deleteLog();

        // Delete the snapshot store.
        storage.deleteSnapshotStore();

        // Delete the metadata store.
        storage.deleteMetaStore();
    }

    @Override
    public String toString() {
        return getClass().getCanonicalName();
    }

    /**
     * Raft server state.
     */
    public enum State {
        ACTIVE,
        READY,
    }

}

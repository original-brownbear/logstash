package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.RaftCluster;
import org.logstash.cluster.protocols.raft.service.RaftService;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * Provides a standalone implementation of the <a href="http://raft.github.io/">Raft consensus algorithm</a>.
 * @see RaftService
 * @see RaftStorage
 */
public class DefaultRaftServer implements RaftServer {

    private static final Logger LOGGER = LogManager.getLogger(DefaultRaftServer.class);

    protected final RaftContext context;
    private volatile CompletableFuture<RaftServer> openFuture;
    private volatile CompletableFuture<Void> closeFuture;
    private volatile boolean started;

    public DefaultRaftServer(RaftContext context) {
        this.context = Preconditions.checkNotNull(context, "context cannot be null");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name())
            .toString();
    }

    @Override
    public String name() {
        return context.getName();
    }

    @Override
    public RaftCluster cluster() {
        return context.getCluster();
    }

    @Override
    public Role getRole() {
        return context.getRole();
    }

    @Override
    public void addRoleChangeListener(Consumer<Role> listener) {
        context.addRoleChangeListener(listener);
    }

    @Override
    public void removeRoleChangeListener(Consumer<Role> listener) {
        context.removeRoleChangeListener(listener);
    }

    @Override
    public CompletableFuture<RaftServer> bootstrap(Collection<MemberId> cluster) {
        return start(() -> cluster().bootstrap(cluster));
    }

    @Override
    public CompletableFuture<RaftServer> join(Collection<MemberId> cluster) {
        return start(() -> cluster().join(cluster));
    }

    @Override
    public CompletableFuture<RaftServer> listen(Collection<MemberId> cluster) {
        return start(() -> cluster().listen(cluster));
    }

    @Override
    public CompletableFuture<RaftServer> promote() {
        return context.anoint().thenApply(v -> this);
    }

    /**
     * Returns a boolean indicating whether the server is running.
     * @return Indicates whether the server is running.
     */
    public boolean isRunning() {
        return started;
    }

    /**
     * Shuts down the server without leaving the Raft cluster.
     * @return A completable future to be completed once the server has been shutdown.
     */
    public CompletableFuture<Void> shutdown() {
        if (!started) {
            return Futures.exceptionalFuture(new IllegalStateException("context not open"));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        context.getThreadContext().execute(() -> {
            started = false;
            context.transition(Role.INACTIVE);
            future.complete(null);
        });

        return future.whenCompleteAsync((result, error) -> {
            context.close();
            started = false;
        });
    }

    /**
     * Leaves the Raft cluster.
     * @return A completable future to be completed once the server has left the cluster.
     */
    public CompletableFuture<Void> leave() {
        if (!started) {
            return CompletableFuture.completedFuture(null);
        }

        if (closeFuture == null) {
            synchronized (this) {
                if (closeFuture == null) {
                    closeFuture = new CompletableFuture<>();
                    if (openFuture == null) {
                        cluster().leave().whenComplete((leaveResult, leaveError) -> {
                            shutdown().whenComplete((shutdownResult, shutdownError) -> {
                                context.delete();
                                closeFuture.complete(null);
                            });
                        });
                    } else {
                        openFuture.whenComplete((openResult, openError) -> {
                            if (openError == null) {
                                cluster().leave().whenComplete((leaveResult, leaveError) -> {
                                    shutdown().whenComplete((shutdownResult, shutdownError) -> {
                                        context.delete();
                                        closeFuture.complete(null);
                                    });
                                });
                            } else {
                                closeFuture.complete(null);
                            }
                        });
                    }
                }
            }
        }

        return closeFuture;
    }

    /**
     * Starts the server.
     */
    private CompletableFuture<RaftServer> start(Supplier<CompletableFuture<Void>> joiner) {
        if (started)
            return CompletableFuture.completedFuture(this);

        if (openFuture == null) {
            synchronized (this) {
                if (openFuture == null) {
                    CompletableFuture<RaftServer> future = new CompletableFuture<>();
                    openFuture = future;
                    joiner.get().whenComplete((result, error) -> {
                        if (error == null) {
                            context.awaitState(RaftContext.State.READY, state -> {
                                started = true;
                                future.complete(null);
                            });
                        } else {
                            future.completeExceptionally(error);
                        }
                    });
                }
            }
        }

        return openFuture.whenComplete((result, error) -> {
            if (error == null) {
                LOGGER.info("Server started successfully!");
            } else {
                LOGGER.warn("Failed to start server!");
            }
        });
    }

    /**
     * Default Raft server builder.
     */
    public static class Builder extends RaftServer.Builder {
        public Builder(MemberId localMemberId) {
            super(localMemberId);
        }

        @Override
        public RaftServer build() {
            if (serviceRegistry.size() == 0) {
                throw new IllegalStateException("No state machines registered");
            }

            // If the server name is null, set it to the member ID.
            if (name == null) {
                name = localMemberId.id();
            }

            // If the storage is not configured, create a new Storage instance with the configured serializer.
            if (storage == null) {
                storage = RaftStorage.builder().build();
            }

            RaftContext raft = new RaftContext(name, localMemberId, protocol, storage, serviceRegistry, threadModel, threadPoolSize);
            raft.setElectionTimeout(electionTimeout);
            raft.setHeartbeatInterval(heartbeatInterval);
            raft.setElectionThreshold(electionThreshold);
            raft.setSessionTimeout(sessionTimeout);
            raft.setSessionFailureThreshold(sessionFailureThreshold);

            return new DefaultRaftServer(raft);
        }
    }
}

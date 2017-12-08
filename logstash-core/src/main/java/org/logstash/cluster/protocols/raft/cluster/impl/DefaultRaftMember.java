package org.logstash.cluster.protocols.raft.cluster.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.hash.Hashing;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import org.logstash.cluster.protocols.raft.RaftError;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureRequest;
import org.logstash.cluster.protocols.raft.storage.system.Configuration;
import org.logstash.cluster.utils.concurrent.Scheduled;

/**
 * Cluster member.
 */
public final class DefaultRaftMember implements RaftMember, AutoCloseable {
    private final MemberId id;
    private final int hash;
    private final transient Set<Consumer<Type>> typeChangeListeners = new CopyOnWriteArraySet<>();
    private Type type;
    private Instant updated;
    private transient Scheduled configureTimeout;
    private transient RaftClusterContext cluster;

    public DefaultRaftMember(MemberId id, Type type, Instant updated) {
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
        this.hash = Hashing.murmur3_32()
            .hashUnencodedChars(id.id())
            .asInt();
        this.type = Preconditions.checkNotNull(type, "type cannot be null");
        this.updated = Preconditions.checkNotNull(updated, "updated cannot be null");
    }

    /**
     * Sets the member's parent cluster.
     */
    DefaultRaftMember setCluster(RaftClusterContext cluster) {
        this.cluster = cluster;
        return this;
    }

    @Override
    public MemberId memberId() {
        return id;
    }

    @Override
    public int hash() {
        return hash;
    }

    @Override
    public RaftMember.Type getType() {
        return type;
    }

    /**
     * Sets the member type.
     * @param type the member type
     */
    void setType(Type type) {
        this.type = type;
    }

    @Override
    public void addTypeChangeListener(Consumer<Type> listener) {
        typeChangeListeners.add(listener);
    }

    @Override
    public void removeTypeChangeListener(Consumer<Type> listener) {
        typeChangeListeners.remove(listener);
    }

    @Override
    public Instant getLastUpdated() {
        return updated;
    }

    @Override
    public CompletableFuture<Void> promote() {
        if (Type.values().length > type.ordinal() + 1) {
            return configure(Type.values()[type.ordinal() + 1]);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> promote(Type type) {
        return configure(type);
    }

    @Override
    public CompletableFuture<Void> demote() {
        if (type.ordinal() > 0) {
            return configure(Type.values()[type.ordinal() - 1]);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> demote(Type type) {
        return configure(type);
    }

    @Override
    public CompletableFuture<Void> remove() {
        return configure(Type.INACTIVE);
    }

    /**
     * Updates the member type.
     * @param type The member type.
     * @return The member.
     */
    public DefaultRaftMember update(RaftMember.Type type, Instant time) {
        if (this.type != type) {
            this.type = Preconditions.checkNotNull(type, "type cannot be null");
            if (time.isAfter(updated)) {
                this.updated = Preconditions.checkNotNull(time, "time cannot be null");
            }
            if (typeChangeListeners != null) {
                typeChangeListeners.forEach(l -> l.accept(type));
            }
        }
        return this;
    }

    /**
     * Demotes the server to the given type.
     */
    private CompletableFuture<Void> configure(RaftMember.Type type) {
        if (type == this.type) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        cluster.getContext().getThreadContext().execute(() -> configure(type, future));
        return future;
    }

    /**
     * Recursively reconfigures the cluster.
     */
    private void configure(RaftMember.Type type, CompletableFuture<Void> future) {
        // Set a timer to retry the attempt to leave the cluster.
        configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout(), () -> {
            configure(type, future);
        });

        // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
        // Non-leader states should forward the request to the leader if there is one. Leader states
        // will log, replicate, and commit the reconfiguration.
        cluster.getContext().getRaftRole().onReconfigure(ReconfigureRequest.builder()
            .withIndex(cluster.getConfiguration().index())
            .withTerm(cluster.getConfiguration().term())
            .withMember(new DefaultRaftMember(id, type, updated))
            .build()).whenComplete((response, error) -> {
            if (error == null) {
                if (response.status() == RaftResponse.Status.OK) {
                    cancelConfigureTimer();
                    cluster.configure(new Configuration(response.index(), response.term(), response.timestamp(), response.members()));
                    future.complete(null);
                } else if (response.error() == null
                    || response.error().type() == RaftError.Type.PROTOCOL_ERROR
                    || response.error().type() == RaftError.Type.NO_LEADER) {
                    cancelConfigureTimer();
                    configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout().multipliedBy(2), () -> configure(type, future));
                } else {
                    cancelConfigureTimer();
                    future.completeExceptionally(response.error().createException());
                }
            } else {
                future.completeExceptionally(error);
            }
        });
    }

    @Override
    public void close() {
        cancelConfigureTimer();
    }

    /**
     * Cancels the configure timeout.
     */
    private void cancelConfigureTimer() {
        if (configureTimeout != null) {
            configureTimeout.cancel();
            configureTimeout = null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), id);
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof DefaultRaftMember && ((DefaultRaftMember) object).id.equals(id);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("type", type)
            .add("updated", updated)
            .toString();
    }

}

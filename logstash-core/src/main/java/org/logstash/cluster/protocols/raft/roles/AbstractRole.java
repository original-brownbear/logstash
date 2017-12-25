package org.logstash.cluster.protocols.raft.roles;

import com.google.common.base.MoreObjects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.impl.DefaultRaftMember;
import org.logstash.cluster.protocols.raft.impl.RaftContext;
import org.logstash.cluster.protocols.raft.protocol.RaftRequest;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * Abstract state.
 */
public abstract class AbstractRole implements RaftRole {

    protected static final Logger LOGGER = LogManager.getLogger(AbstractRole.class);

    protected final RaftContext raft;
    private boolean open = true;

    protected AbstractRole(RaftContext raft) {
        this.raft = raft;
    }

    /**
     * Returns the Raft state represented by this state.
     * @return The Raft state represented by this state.
     */
    @Override
    public abstract RaftServer.Role role();

    /**
     * Logs a request.
     */
    protected final <R extends RaftRequest> R logRequest(R request) {
        LOGGER.trace("Received {}", request);
        return request;
    }

    /**
     * Logs a response.
     */
    protected final <R extends RaftResponse> R logResponse(R response) {
        LOGGER.trace("Sending {}", response);
        return response;
    }

    @Override
    public CompletableFuture<RaftRole> open() {
        raft.checkThread();
        open = true;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public CompletableFuture<Void> close() {
        raft.checkThread();
        open = false;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isClosed() {
        return !open;
    }

    /**
     * Forwards the given request to the leader if possible.
     */
    protected <T extends RaftRequest, U extends RaftResponse> CompletableFuture<U> forward(T request, BiFunction<MemberId, T, CompletableFuture<U>> function) {
        CompletableFuture<U> future = new CompletableFuture<>();
        DefaultRaftMember leader = raft.getLeader();
        if (leader == null) {
            return Futures.exceptionalFuture(new RaftException.NoLeader("No leader found"));
        }

        function.apply(leader.memberId(), request).whenCompleteAsync((response, error) -> {
            if (error == null) {
                future.complete(response);
            } else {
                future.completeExceptionally(error);
            }
        }, raft.getThreadContext());
        return future;
    }

    /**
     * Updates the term and leader.
     */
    protected boolean updateTermAndLeader(long term, MemberId leader) {
        // If the request indicates a term that is greater than the current term or no leader has been
        // set for the current term, update leader and term.
        if (term > raft.getTerm() || (term == raft.getTerm() && raft.getLeader() == null && leader != null)) {
            raft.setTerm(term);
            raft.setLeader(leader);

            // Reset the current cluster configuration to the last committed configuration when a leader change occurs.
            raft.getCluster().reset();
            return true;
        }

        // If the leader is non-null, update the last heartbeat time.
        if (leader != null) {
            raft.setLastHeartbeatTime();
        }
        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("context", raft)
            .toString();
    }

}

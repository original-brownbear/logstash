package org.logstash.cluster.protocols.raft.roles;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.logstash.cluster.protocols.phi.PhiAccrualFailureDetector;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.cluster.impl.DefaultRaftMember;
import org.logstash.cluster.protocols.raft.cluster.impl.RaftMemberContext;
import org.logstash.cluster.protocols.raft.impl.RaftContext;
import org.logstash.cluster.protocols.raft.protocol.PollRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteResponse;
import org.logstash.cluster.protocols.raft.storage.log.entry.RaftLogEntry;
import org.logstash.cluster.protocols.raft.utils.Quorum;
import org.logstash.cluster.storage.journal.Indexed;
import org.logstash.cluster.utils.concurrent.Scheduled;

/**
 * Follower state.
 */
public final class FollowerRole extends ActiveRole {
    private final PhiAccrualFailureDetector failureDetector = new PhiAccrualFailureDetector();
    private final Random random = new Random();
    private Scheduled heartbeatTimer;
    private Scheduled heartbeatTimeout;

    public FollowerRole(RaftContext context) {
        super(context);
    }

    @Override
    public synchronized CompletableFuture<RaftRole> open() {
        raft.setLastHeartbeatTime();
        return super.open().thenRun(this::startHeartbeatTimer).thenApply(v -> this);
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        return super.close().thenRun(this::cancelHeartbeatTimers);
    }

    @Override
    public RaftServer.Role role() {
        return RaftServer.Role.FOLLOWER;
    }

    /**
     * Starts the heartbeat timer.
     */
    private void startHeartbeatTimer() {
        LOGGER.trace("Starting heartbeat timer");
        AtomicLong lastHeartbeat = new AtomicLong();
        heartbeatTimer = raft.getThreadContext().schedule(raft.getHeartbeatInterval(), () -> {
            if (raft.getLastHeartbeatTime() > lastHeartbeat.get()) {
                failureDetector.report(raft.getLastHeartbeatTime());
            }
            lastHeartbeat.set(raft.getLastHeartbeatTime());
        });
        resetHeartbeatTimeout();
    }

    /**
     * Resets the heartbeat timer.
     */
    private void resetHeartbeatTimeout() {
        Duration delay = raft.getHeartbeatInterval().dividedBy(2)
            .plus(Duration.ofMillis(random.nextInt((int) raft.getHeartbeatInterval().dividedBy(2).toMillis())));
        heartbeatTimeout = raft.getThreadContext().schedule(delay, () -> {
            if (isOpen()) {
                if (System.currentTimeMillis() - raft.getLastHeartbeatTime() > raft.getElectionTimeout().toMillis() || failureDetector.phi() >= raft.getElectionThreshold()) {
                    LOGGER.debug("Heartbeat timed out in {}", System.currentTimeMillis() - raft.getLastHeartbeatTime());
                    sendPollRequests();
                } else {
                    resetHeartbeatTimeout();
                }
            }
        });
    }

    /**
     * Polls all members of the cluster to determine whether this member should transition to the CANDIDATE state.
     */
    private void sendPollRequests() {
        // Set a new timer within which other nodes must respond in order for this node to transition to candidate.
        heartbeatTimeout = raft.getThreadContext().schedule(raft.getElectionTimeout(), () -> {
            LOGGER.debug("Failed to poll a majority of the cluster in {}", raft.getElectionTimeout());
            resetHeartbeatTimeout();
        });

        // Create a quorum that will track the number of nodes that have responded to the poll request.
        final AtomicBoolean complete = new AtomicBoolean();
        final Set<DefaultRaftMember> votingMembers = new HashSet<>(raft.getCluster().getActiveMemberStates().stream().map(RaftMemberContext::getMember).collect(Collectors.toList()));

        // If there are no other members in the cluster, immediately transition to leader.
        if (votingMembers.isEmpty()) {
            raft.setLeader(null);
            raft.transition(RaftServer.Role.CANDIDATE);
            return;
        }

        final Quorum quorum = new Quorum(raft.getCluster().getQuorum(), (elected) -> {
            // If a majority of the cluster indicated they would vote for us then transition to candidate.
            complete.set(true);
            if (elected) {
                raft.setLeader(null);
                raft.transition(RaftServer.Role.CANDIDATE);
            } else {
                resetHeartbeatTimeout();
            }
        });

        // First, load the last log entry to get its term. We load the entry
        // by its index since the index is required by the protocol.
        final Indexed<RaftLogEntry> lastEntry = raft.getLogWriter().getLastEntry();

        final long lastTerm;
        if (lastEntry != null) {
            lastTerm = lastEntry.entry().term();
        } else {
            lastTerm = 0;
        }

        final DefaultRaftMember leader = raft.getLeader();

        LOGGER.debug("Polling members {}", votingMembers);

        // Once we got the last log term, iterate through each current member
        // of the cluster and vote each member for a vote.
        for (DefaultRaftMember member : votingMembers) {
            LOGGER.debug("Polling {} for next term {}", member, raft.getTerm() + 1);
            PollRequest request = PollRequest.builder()
                .withTerm(raft.getTerm())
                .withCandidate(raft.getCluster().getMember().memberId())
                .withLastLogIndex(lastEntry != null ? lastEntry.index() : 0)
                .withLastLogTerm(lastTerm)
                .build();
            raft.getProtocol().poll(member.memberId(), request).whenCompleteAsync((response, error) -> {
                raft.checkThread();
                if (isOpen() && !complete.get()) {
                    if (error != null) {
                        LOGGER.warn("{}", error.getMessage());
                        quorum.fail();
                    } else {
                        if (response.term() > raft.getTerm()) {
                            raft.setTerm(response.term());
                        }

                        if (!response.accepted()) {
                            LOGGER.debug("Received rejected poll from {}", member);
                            if (leader != null && response.term() == raft.getTerm() && member.memberId().equals(leader.memberId())) {
                                quorum.cancel();
                                resetHeartbeatTimeout();
                            } else {
                                quorum.fail();
                            }
                        } else if (response.term() != raft.getTerm()) {
                            LOGGER.debug("Received accepted poll for a different term from {}", member);
                            quorum.fail();
                        } else {
                            LOGGER.debug("Received accepted poll from {}", member);
                            quorum.succeed();
                        }
                    }
                }
            }, raft.getThreadContext());
        }
    }

    @Override
    protected VoteResponse handleVote(VoteRequest request) {
        // Reset the heartbeat timeout if we voted for another candidate.
        VoteResponse response = super.handleVote(request);
        if (response.voted()) {
            raft.setLastHeartbeatTime();
        }
        return response;
    }

    /**
     * Cancels the heartbeat timer.
     */
    private void cancelHeartbeatTimers() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }
        if (heartbeatTimeout != null) {
            LOGGER.trace("Cancelling heartbeat timer");
            heartbeatTimeout.cancel();
        }
    }

}

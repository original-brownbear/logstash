/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.logstash.cluster.protocols.raft.roles;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.cluster.impl.DefaultRaftMember;
import org.logstash.cluster.protocols.raft.cluster.impl.RaftMemberContext;
import org.logstash.cluster.protocols.raft.impl.RaftContext;
import org.logstash.cluster.protocols.raft.protocol.AppendRequest;
import org.logstash.cluster.protocols.raft.protocol.AppendResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.protocol.VoteRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteResponse;
import org.logstash.cluster.protocols.raft.storage.log.entry.RaftLogEntry;
import org.logstash.cluster.protocols.raft.utils.Quorum;
import org.logstash.cluster.storage.journal.Indexed;
import org.logstash.cluster.utils.concurrent.Scheduled;

/**
 * Candidate state.
 */
public final class CandidateRole extends ActiveRole {
    private final Random random = new Random();
    private Quorum quorum;
    private Scheduled currentTimer;

    public CandidateRole(RaftContext context) {
        super(context);
    }

    @Override
    public synchronized CompletableFuture<RaftRole> open() {
        return super.open().thenRun(this::startElection).thenApply(v -> this);
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        return super.close().thenRun(this::cancelElection);
    }

    @Override
    public RaftServer.Role role() {
        return RaftServer.Role.CANDIDATE;
    }

    /**
     * Starts the election.
     */
    void startElection() {
        LOGGER.info("Starting election");
        sendVoteRequests();
    }

    /**
     * Resets the election timer.
     */
    private void sendVoteRequests() {
        raft.checkThread();

        // Because of asynchronous execution, the candidate state could have already been closed. In that case,
        // simply skip the election.
        if (isClosed()) {
            return;
        }

        // Cancel the current timer task and purge the election timer of cancelled tasks.
        if (currentTimer != null) {
            currentTimer.cancel();
        }

        // When the election timer is reset, increment the current term and
        // restart the election.
        raft.setTerm(raft.getTerm() + 1);
        raft.setLastVotedFor(raft.getCluster().getMember().memberId());

        Duration delay = raft.getElectionTimeout().plus(Duration.ofMillis(random.nextInt((int) raft.getElectionTimeout().toMillis())));
        currentTimer = raft.getThreadContext().schedule(delay, () -> {
            // When the election times out, clear the previous majority vote
            // check and restart the election.
            LOGGER.debug("Election timed out");
            if (quorum != null) {
                quorum.cancel();
                quorum = null;
            }
            sendVoteRequests();
            LOGGER.debug("Restarted election");
        });

        final AtomicBoolean complete = new AtomicBoolean();
        final Set<DefaultRaftMember> votingMembers = new HashSet<>(raft.getCluster().getActiveMemberStates().stream().map(RaftMemberContext::getMember).collect(Collectors.toList()));

        // If there are no other members in the cluster, immediately transition to leader.
        if (votingMembers.isEmpty()) {
            LOGGER.debug("Single member cluster. Transitioning directly to leader.", raft.getCluster().getMember().memberId());
            raft.transition(RaftServer.Role.LEADER);
            return;
        }

        // Send vote requests to all nodes. The vote request that is sent
        // to this node will be automatically successful.
        // First check if the quorum is null. If the quorum isn't null then that
        // indicates that another vote is already going on.
        final Quorum quorum = new Quorum(raft.getCluster().getQuorum(), (elected) -> {
            complete.set(true);
            if (elected) {
                raft.transition(RaftServer.Role.LEADER);
            } else {
                raft.transition(RaftServer.Role.FOLLOWER);
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

        LOGGER.debug("Requesting votes for term {}", raft.getTerm());

        // Once we got the last log term, iterate through each current member
        // of the cluster and vote each member for a vote.
        for (DefaultRaftMember member : votingMembers) {
            LOGGER.debug("Requesting vote from {} for term {}", member, raft.getTerm());
            VoteRequest request = VoteRequest.builder()
                .withTerm(raft.getTerm())
                .withCandidate(raft.getCluster().getMember().memberId())
                .withLastLogIndex(lastEntry != null ? lastEntry.index() : 0)
                .withLastLogTerm(lastTerm)
                .build();

            raft.getProtocol().vote(member.memberId(), request).whenCompleteAsync((response, error) -> {
                raft.checkThread();
                if (isOpen() && !complete.get()) {
                    if (error != null) {
                        LOGGER.warn(error.getMessage());
                        quorum.fail();
                    } else {
                        if (response.term() > raft.getTerm()) {
                            LOGGER.debug("Received greater term from {}", member);
                            raft.setTerm(response.term());
                            complete.set(true);
                            raft.transition(RaftServer.Role.FOLLOWER);
                        } else if (!response.voted()) {
                            LOGGER.debug("Received rejected vote from {}", member);
                            quorum.fail();
                        } else if (response.term() != raft.getTerm()) {
                            LOGGER.debug("Received successful vote for a different term from {}", member);
                            quorum.fail();
                        } else {
                            LOGGER.debug("Received successful vote from {}", member);
                            quorum.succeed();
                        }
                    }
                }
            }, raft.getThreadContext());
        }
    }

    @Override
    public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
        raft.checkThread();

        // If the request indicates a term that is greater than the current term then
        // assign that term and leader to the current context and step down as a candidate.
        if (request.term() >= raft.getTerm()) {
            raft.setTerm(request.term());
            raft.transition(RaftServer.Role.FOLLOWER);
        }
        return super.onAppend(request);
    }

    @Override
    public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
        raft.checkThread();
        logRequest(request);

        // If the request indicates a term that is greater than the current term then
        // assign that term and leader to the current context and step down as a candidate.
        if (updateTermAndLeader(request.term(), null)) {
            CompletableFuture<VoteResponse> future = super.onVote(request);
            raft.transition(RaftServer.Role.FOLLOWER);
            return future;
        }

        // If the vote request is not for this candidate then reject the vote.
        if (request.candidate() == raft.getCluster().getMember().memberId()) {
            return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
                .withStatus(RaftResponse.Status.OK)
                .withTerm(raft.getTerm())
                .withVoted(true)
                .build()));
        } else {
            return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
                .withStatus(RaftResponse.Status.OK)
                .withTerm(raft.getTerm())
                .withVoted(false)
                .build()));
        }
    }

    /**
     * Cancels the election.
     */
    private void cancelElection() {
        raft.checkThread();
        if (currentTimer != null) {
            LOGGER.debug("Cancelling election");
            currentTimer.cancel();
        }
        if (quorum != null) {
            quorum.cancel();
            quorum = null;
        }
    }

}
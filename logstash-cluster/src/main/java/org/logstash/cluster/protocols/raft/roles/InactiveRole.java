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

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.impl.RaftContext;
import org.logstash.cluster.protocols.raft.protocol.AppendRequest;
import org.logstash.cluster.protocols.raft.protocol.AppendResponse;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.CommandRequest;
import org.logstash.cluster.protocols.raft.protocol.CommandResponse;
import org.logstash.cluster.protocols.raft.protocol.ConfigureRequest;
import org.logstash.cluster.protocols.raft.protocol.ConfigureResponse;
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
import org.logstash.cluster.protocols.raft.protocol.QueryRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureRequest;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureResponse;
import org.logstash.cluster.protocols.raft.protocol.TransferRequest;
import org.logstash.cluster.protocols.raft.protocol.TransferResponse;
import org.logstash.cluster.protocols.raft.protocol.VoteRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteResponse;
import org.logstash.cluster.protocols.raft.storage.system.Configuration;
import org.logstash.cluster.utils.concurrent.Futures;

/**
 * Inactive state.
 */
public class InactiveRole extends AbstractRole {

    public InactiveRole(RaftContext context) {
        super(context);
    }

    @Override
    public RaftServer.Role role() {
        return RaftServer.Role.INACTIVE;
    }

    @Override
    public CompletableFuture<MetadataResponse> onMetadata(MetadataRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<OpenSessionResponse> onOpenSession(OpenSessionRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<CloseSessionResponse> onCloseSession(CloseSessionRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request) {
        raft.checkThread();
        logRequest(request);
        updateTermAndLeader(request.term(), request.leader());

        Configuration configuration = new Configuration(request.index(), request.term(), request.timestamp(), request.members());

        // Configure the cluster membership. This will cause this server to transition to the
        // appropriate state if its type has changed.
        raft.getCluster().configure(configuration);

        // If the configuration is already committed, commit it to disk.
        // Check against the actual cluster Configuration rather than the received configuration in
        // case the received configuration was an older configuration that was not applied.
        if (raft.getCommitIndex() >= raft.getCluster().getConfiguration().index()) {
            raft.getCluster().commit();
        }

        return CompletableFuture.completedFuture(logResponse(ConfigureResponse.builder()
            .withStatus(RaftResponse.Status.OK)
            .build()));
    }

    @Override
    public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<JoinResponse> onJoin(JoinRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<TransferResponse> onTransfer(TransferRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<PollResponse> onPoll(PollRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<CommandResponse> onCommand(CommandRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

    @Override
    public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
        return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
    }

}

/*
 * Copyright 2016-present Open Networking Foundation
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
 * limitations under the License
 */
package org.logstash.cluster.protocols.raft.roles;

import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.protocols.raft.RaftServer;
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
import org.logstash.cluster.protocols.raft.protocol.ReconfigureRequest;
import org.logstash.cluster.protocols.raft.protocol.ReconfigureResponse;
import org.logstash.cluster.protocols.raft.protocol.TransferRequest;
import org.logstash.cluster.protocols.raft.protocol.TransferResponse;
import org.logstash.cluster.protocols.raft.protocol.VoteRequest;
import org.logstash.cluster.protocols.raft.protocol.VoteResponse;
import org.logstash.cluster.utils.Managed;

/**
 * Raft role interface.
 */
public interface RaftRole extends Managed<RaftRole> {

    /**
     * Returns the server state type.
     * @return The server state type.
     */
    RaftServer.Role role();

    /**
     * Handles a metadata request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<MetadataResponse> onMetadata(MetadataRequest request);

    /**
     * Handles an open session request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<OpenSessionResponse> onOpenSession(OpenSessionRequest request);

    /**
     * Handles a keep alive request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request);

    /**
     * Handles a close session request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<CloseSessionResponse> onCloseSession(CloseSessionRequest request);

    /**
     * Handles a configure request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request);

    /**
     * Handles an install request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<InstallResponse> onInstall(InstallRequest request);

    /**
     * Handles a join request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<JoinResponse> onJoin(JoinRequest request);

    /**
     * Handles a configure request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request);

    /**
     * Handles a leave request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<LeaveResponse> onLeave(LeaveRequest request);

    /**
     * Handles a transfer request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<TransferResponse> onTransfer(TransferRequest request);

    /**
     * Handles an append request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<AppendResponse> onAppend(AppendRequest request);

    /**
     * Handles a poll request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<PollResponse> onPoll(PollRequest request);

    /**
     * Handles a vote request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<VoteResponse> onVote(VoteRequest request);

    /**
     * Handles a command request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<CommandResponse> onCommand(CommandRequest request);

    /**
     * Handles a query request.
     * @param request The request to handle.
     * @return A completable future to be completed with the request response.
     */
    CompletableFuture<QueryResponse> onQuery(QueryRequest request);

}

/*
 * Copyright 2017-present Open Networking Foundation
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
package org.logstash.cluster.messaging;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.CloseSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.CommandRequest;
import org.logstash.cluster.protocols.raft.protocol.CommandResponse;
import org.logstash.cluster.protocols.raft.protocol.HeartbeatRequest;
import org.logstash.cluster.protocols.raft.protocol.HeartbeatResponse;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveRequest;
import org.logstash.cluster.protocols.raft.protocol.KeepAliveResponse;
import org.logstash.cluster.protocols.raft.protocol.MetadataRequest;
import org.logstash.cluster.protocols.raft.protocol.MetadataResponse;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionRequest;
import org.logstash.cluster.protocols.raft.protocol.OpenSessionResponse;
import org.logstash.cluster.protocols.raft.protocol.PublishRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryRequest;
import org.logstash.cluster.protocols.raft.protocol.QueryResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.protocol.ResetRequest;
import org.logstash.cluster.protocols.raft.session.SessionId;
import org.logstash.cluster.serializer.Serializer;

/**
 * Raft client protocol that uses a cluster communicator.
 */
public class TestRaftClientCommunicator implements RaftClientProtocol {
    private final TestRaftMessageContext context;
    private final Serializer serializer;
    private final ClusterCommunicationService clusterCommunicator;

    public TestRaftClientCommunicator(Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this(null, serializer, clusterCommunicator);
    }

    public TestRaftClientCommunicator(String prefix, Serializer serializer, ClusterCommunicationService clusterCommunicator) {
        this.context = new TestRaftMessageContext(prefix);
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
    public void reset(Collection<MemberId> members, ResetRequest request) {
        Set<NodeId> nodes = members.stream().map(m -> NodeId.from(m.id())).collect(Collectors.toSet());
        clusterCommunicator.multicast(context.resetSubject(request.session()), request, serializer::encode, nodes);
    }

    @Override
    public void registerHeartbeatHandler(Function<HeartbeatRequest, CompletableFuture<HeartbeatResponse>> handler) {
        clusterCommunicator.addSubscriber(context.heartbeatSubject, serializer::decode, handler, serializer::encode);
    }

    @Override
    public void unregisterHeartbeatHandler() {
        clusterCommunicator.removeSubscriber(context.heartbeatSubject);
    }

    @Override
    public void registerPublishListener(SessionId sessionId, Consumer<PublishRequest> listener, Executor executor) {
        clusterCommunicator.addSubscriber(context.publishSubject(sessionId.id()), serializer::decode, listener, executor);
    }

    @Override
    public void unregisterPublishListener(SessionId sessionId) {
        clusterCommunicator.removeSubscriber(context.publishSubject(sessionId.id()));
    }
}
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
package org.logstash.cluster.protocols.raft.impl;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.logstash.cluster.protocols.raft.RaftClient;
import org.logstash.cluster.protocols.raft.RaftMetadataClient;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.protocol.MetadataRequest;
import org.logstash.cluster.protocols.raft.protocol.MetadataResponse;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.protocol.RaftResponse;
import org.logstash.cluster.protocols.raft.proxy.CommunicationStrategy;
import org.logstash.cluster.protocols.raft.proxy.impl.MemberSelectorManager;
import org.logstash.cluster.protocols.raft.proxy.impl.RaftProxyConnection;
import org.logstash.cluster.protocols.raft.service.ServiceType;
import org.logstash.cluster.protocols.raft.session.RaftSessionMetadata;
import org.logstash.cluster.utils.concurrent.ThreadContext;
import org.logstash.cluster.utils.logging.LoggerContext;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default Raft metadata.
 */
public class DefaultRaftMetadataClient implements RaftMetadataClient {
    private final MemberSelectorManager selectorManager;
    private final RaftProxyConnection connection;

    public DefaultRaftMetadataClient(String clientId, RaftClientProtocol protocol, MemberSelectorManager selectorManager, ThreadContext context) {
        this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
        this.connection = new RaftProxyConnection(
            protocol,
            selectorManager.createSelector(CommunicationStrategy.LEADER),
            context,
            LoggerContext.builder(RaftClient.class)
                .addValue(clientId)
                .build());
    }

    @Override
    public MemberId getLeader() {
        return selectorManager.leader();
    }

    @Override
    public Collection<MemberId> getMembers() {
        return selectorManager.members();
    }

    /**
     * Requests metadata from the cluster.
     * @return A completable future to be completed with cluster metadata.
     */
    private CompletableFuture<MetadataResponse> getMetadata() {
        CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
        connection.metadata(MetadataRequest.builder().build()).whenComplete((response, error) -> {
            if (error == null) {
                if (response.status() == RaftResponse.Status.OK) {
                    future.complete(response);
                } else {
                    future.completeExceptionally(response.error().createException());
                }
            } else {
                future.completeExceptionally(error);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Set<RaftSessionMetadata>> getSessions() {
        return getMetadata().thenApply(MetadataResponse::sessions);
    }

    @Override
    public CompletableFuture<Set<RaftSessionMetadata>> getSessions(ServiceType serviceType) {
        return getMetadata().thenApply(response -> response.sessions()
            .stream()
            .filter(s -> s.serviceType().id().equals(serviceType.id()))
            .collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Set<RaftSessionMetadata>> getSessions(ServiceType serviceType, String serviceName) {
        return getMetadata().thenApply(response -> response.sessions()
            .stream()
            .filter(s -> s.serviceType().id().equals(serviceType.id()) && s.serviceName().equals(serviceName))
            .collect(Collectors.toSet()));
    }
}

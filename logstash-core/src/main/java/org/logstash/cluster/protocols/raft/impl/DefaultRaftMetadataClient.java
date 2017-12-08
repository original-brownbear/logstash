package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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

/**
 * Default Raft metadata.
 */
public class DefaultRaftMetadataClient implements RaftMetadataClient {
    private final MemberSelectorManager selectorManager;
    private final RaftProxyConnection connection;

    public DefaultRaftMetadataClient(final RaftClientProtocol protocol, final MemberSelectorManager selectorManager, final ThreadContext context) {
        this.selectorManager = Preconditions.checkNotNull(selectorManager, "selectorManager cannot be null");
        this.connection = new RaftProxyConnection(
            protocol, selectorManager.createSelector(CommunicationStrategy.LEADER), context
        );
    }

    @Override
    public MemberId getLeader() {
        return selectorManager.leader();
    }

    @Override
    public Collection<MemberId> getMembers() {
        return selectorManager.members();
    }

    @Override
    public CompletableFuture<Set<RaftSessionMetadata>> getSessions() {
        return getMetadata().thenApply(MetadataResponse::sessions);
    }

    /**
     * Requests metadata from the cluster.
     * @return A completable future to be completed with cluster metadata.
     */
    private CompletableFuture<MetadataResponse> getMetadata() {
        final CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
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
    public CompletableFuture<Set<RaftSessionMetadata>> getSessions(final ServiceType serviceType) {
        return getMetadata().thenApply(response -> response.sessions()
            .stream()
            .filter(s -> s.serviceType().id().equals(serviceType.id()))
            .collect(Collectors.toSet()));
    }

    @Override
    public CompletableFuture<Set<RaftSessionMetadata>> getSessions(final ServiceType serviceType, final String serviceName) {
        return getMetadata().thenApply(response -> response.sessions()
            .stream()
            .filter(s -> s.serviceType().id().equals(serviceType.id()) && s.serviceName().equals(serviceName))
            .collect(Collectors.toSet()));
    }
}

package org.logstash.cluster.protocols.raft.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.logging.log4j.LogManager;
import org.logstash.cluster.protocols.raft.RaftClient;
import org.logstash.cluster.protocols.raft.RaftMetadataClient;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxyClient;
import org.logstash.cluster.protocols.raft.proxy.RecoveryStrategy;
import org.logstash.cluster.protocols.raft.proxy.impl.BlockingAwareRaftProxyClient;
import org.logstash.cluster.protocols.raft.proxy.impl.DelegatingRaftProxy;
import org.logstash.cluster.protocols.raft.proxy.impl.MemberSelectorManager;
import org.logstash.cluster.protocols.raft.proxy.impl.RaftProxyManager;
import org.logstash.cluster.protocols.raft.proxy.impl.RecoveringRaftProxyClient;
import org.logstash.cluster.protocols.raft.proxy.impl.RetryingRaftProxyClient;
import org.logstash.cluster.utils.concurrent.ThreadContext;
import org.logstash.cluster.utils.concurrent.ThreadContextFactory;

/**
 * Default Raft client implementation.
 */
public class DefaultRaftClient implements RaftClient {
    private final String clientId;
    private final Collection<MemberId> cluster;
    private final ThreadContextFactory threadContextFactory;
    private final ThreadContext threadContext;
    private final RaftMetadataClient metadata;
    private final MemberSelectorManager selectorManager = new MemberSelectorManager();
    private final RaftProxyManager sessionManager;

    public DefaultRaftClient(
        final String clientId,
        final MemberId nodeId,
        final Collection<MemberId> cluster,
        final RaftClientProtocol protocol,
        final ThreadContextFactory threadContextFactory) {
        this.clientId = Preconditions.checkNotNull(clientId, "clientId cannot be null");
        this.cluster = Preconditions.checkNotNull(cluster, "cluster cannot be null");
        this.threadContextFactory = Preconditions.checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
        this.threadContext = threadContextFactory.createContext();
        this.metadata = new DefaultRaftMetadataClient(protocol, selectorManager, threadContextFactory.createContext());
        this.sessionManager = new RaftProxyManager(clientId, nodeId, protocol, selectorManager, threadContextFactory);
    }

    @Override
    public String clientId() {
        return clientId;
    }

    @Override
    public RaftMetadataClient metadata() {
        return metadata;
    }

    @Override
    public RaftProxy.Builder newProxyBuilder() {
        return new ProxyBuilder();
    }

    @Override
    public synchronized CompletableFuture<RaftClient> connect(Collection<MemberId> cluster) {
        final CompletableFuture<RaftClient> future = new CompletableFuture<>();

        // If the provided cluster list is null or empty, use the default list.
        if (cluster == null || cluster.isEmpty()) {
            cluster = this.cluster;
        }

        // If the default list is null or empty, use the default host:port.
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("No cluster specified");
        }

        // Reset the connection list to allow the selection strategy to prioritize connections.
        sessionManager.resetConnections(null, cluster);

        // Register the session manager.
        sessionManager.open().whenCompleteAsync((result, error) -> {
            if (error == null) {
                future.complete(this);
            } else {
                future.completeExceptionally(error);
            }
        }, threadContext);
        return future;
    }

    @Override
    public synchronized CompletableFuture<Void> close() {
        return sessionManager.close().thenRunAsync(threadContextFactory::close);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", clientId)
            .toString();
    }

    /**
     * Default Raft client builder.
     */
    public static class Builder extends RaftClient.Builder {
        public Builder(final Collection<MemberId> cluster) {
            super(cluster);
        }

        @Override
        public RaftClient build() {
            Preconditions.checkNotNull(nodeId, "nodeId cannot be null");
            return new DefaultRaftClient(
                clientId, nodeId, cluster, protocol,
                threadModel.factory(
                    "raft-client-" + clientId + "-%d", threadPoolSize,
                    LogManager.getLogger(DefaultRaftClient.class)
                )
            );
        }
    }

    /**
     * Default Raft session builder.
     */
    private class ProxyBuilder extends RaftProxy.Builder {
        @Override
        public RaftProxy build() {
            // Create a proxy builder that uses the session manager to open a session.
            final RaftProxyClient.Builder clientBuilder = new RaftProxyClient.Builder() {
                @Override
                public CompletableFuture<RaftProxyClient> buildAsync() {
                    return sessionManager.openSession(name, serviceType, readConsistency, communicationStrategy, minTimeout, maxTimeout);
                }
            };

            // Populate the proxy client builder.
            clientBuilder.withName(name)
                .withServiceType(serviceType)
                .withReadConsistency(readConsistency)
                .withMaxRetries(maxRetries)
                .withRetryDelay(retryDelay)
                .withCommunicationStrategy(communicationStrategy)
                .withRecoveryStrategy(recoveryStrategy)
                .withMinTimeout(minTimeout)
                .withMaxTimeout(maxTimeout);

            RaftProxyClient client;

            // If the recovery strategy is set to RECOVER, wrap the builder in a recovering proxy client.
            if (recoveryStrategy == RecoveryStrategy.RECOVER) {
                client = new RecoveringRaftProxyClient(clientId, name, serviceType, clientBuilder, threadContextFactory.createContext());
            } else {
                client = clientBuilder.build();
            }

            // If max retries is set, wrap the client in a retrying proxy client.
            if (maxRetries > 0) {
                client = new RetryingRaftProxyClient(client, threadContextFactory.createContext(), maxRetries, retryDelay);
            }

            // Default the executor to use the configured thread pool executor and create a blocking aware proxy client.
            final Executor executor = this.executor != null ? this.executor : threadContextFactory.createContext();
            client = new BlockingAwareRaftProxyClient(client, executor);

            // Create the proxy.
            return new DelegatingRaftProxy(client);
        }
    }
}

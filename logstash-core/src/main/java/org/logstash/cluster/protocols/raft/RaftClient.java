package org.logstash.cluster.protocols.raft;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.impl.DefaultRaftClient;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.proxy.CommunicationStrategy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;

/**
 * Provides an interface for submitting operations to the Raft cluster.
 */
public interface RaftClient {

    /**
     * Returns a new Raft client builder.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
     * the cluster's leader.
     * @return The client builder.
     */
    @SuppressWarnings("unchecked")
    static RaftClient.Builder builder() {
        return builder(Collections.emptyList());
    }

    /**
     * Returns a new Raft client builder.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
     * the cluster's leader.
     * @param cluster The cluster to which to connect.
     * @return The client builder.
     */
    static RaftClient.Builder builder(Collection<MemberId> cluster) {
        return new DefaultRaftClient.Builder(cluster);
    }

    /**
     * Returns a new Raft client builder.
     * <p>
     * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
     * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
     * the cluster's leader.
     * @param cluster The cluster to which to connect.
     * @return The client builder.
     */
    static RaftClient.Builder builder(MemberId... cluster) {
        return builder(Arrays.asList(cluster));
    }

    /**
     * Returns the globally unique client identifier.
     * @return the globally unique client identifier
     */
    String clientId();

    /**
     * Returns the Raft metadata.
     * @return The Raft metadata.
     */
    RaftMetadataClient metadata();

    /**
     * Returns a new proxy builder.
     * @return A new proxy builder.
     */
    RaftProxy.Builder newProxyBuilder();

    /**
     * Connects the client to Raft cluster via the provided server addresses.
     * <p>
     * The client will connect to servers in the cluster according to the pattern specified by the configured
     * {@link CommunicationStrategy}.
     * @param members A set of server addresses to which to connect.
     * @return A completable future to be completed once the client is registered.
     */
    default CompletableFuture<RaftClient> connect(MemberId... members) {
        if (members == null || members.length == 0) {
            return connect();
        } else {
            return connect(Arrays.asList(members));
        }
    }

    /**
     * Connects the client to Raft cluster via the default server address.
     * <p>
     * If the client was built with a default cluster list, the default server addresses will be used. Otherwise, the client
     * will attempt to connect to localhost:8700.
     * <p>
     * The client will connect to servers in the cluster according to the pattern specified by the configured
     * {@link CommunicationStrategy}.
     * @return A completable future to be completed once the client is registered.
     */
    default CompletableFuture<RaftClient> connect() {
        return connect((Collection<MemberId>) null);
    }

    /**
     * Connects the client to Raft cluster via the provided server addresses.
     * <p>
     * The client will connect to servers in the cluster according to the pattern specified by the configured
     * {@link CommunicationStrategy}.
     * @param members A set of server addresses to which to connect.
     * @return A completable future to be completed once the client is registered.
     */
    CompletableFuture<RaftClient> connect(Collection<MemberId> members);

    /**
     * Closes the client.
     * @return A completable future to be completed once the client has been closed.
     */
    CompletableFuture<Void> close();

    /**
     * Builds a new Raft client.
     * <p>
     * New client builders should be constructed using the static {@link #builder()} factory method.
     * <pre>
     *   {@code
     *     RaftClient client = RaftClient.builder(new Address("123.456.789.0", 5000), new Address("123.456.789.1", 5000)
     *       .withTransport(new NettyTransport())
     *       .build();
     *   }
     * </pre>
     */
    abstract class Builder implements org.logstash.cluster.utils.Builder<RaftClient> {
        protected final Collection<MemberId> cluster;
        protected String clientId = UUID.randomUUID().toString();
        protected MemberId nodeId;
        protected RaftClientProtocol protocol;
        protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
        protected int threadPoolSize = Runtime.getRuntime().availableProcessors();

        protected Builder(Collection<MemberId> cluster) {
            this.cluster = Preconditions.checkNotNull(cluster, "cluster cannot be null");
        }

        /**
         * Sets the client ID.
         * <p>
         * The client ID is a name that should be unique among all clients. The ID will be used to resolve
         * and recover sessions.
         * @param clientId The client ID.
         * @return The client builder.
         * @throws NullPointerException if {@code clientId} is null
         */
        public RaftClient.Builder withClientId(String clientId) {
            this.clientId = Preconditions.checkNotNull(clientId, "clientId cannot be null");
            return this;
        }

        /**
         * Sets the local node identifier.
         * @param nodeId The local node identifier.
         * @return The client builder.
         * @throws NullPointerException if {@code nodeId} is null
         */
        public RaftClient.Builder withMemberId(MemberId nodeId) {
            this.nodeId = Preconditions.checkNotNull(nodeId, "nodeId cannot be null");
            return this;
        }

        /**
         * Sets the client protocol.
         * @param protocol the client protocol
         * @return the client builder
         * @throws NullPointerException if the protocol is null
         */
        public RaftClient.Builder withProtocol(RaftClientProtocol protocol) {
            this.protocol = Preconditions.checkNotNull(protocol, "protocol cannot be null");
            return this;
        }

    }
}

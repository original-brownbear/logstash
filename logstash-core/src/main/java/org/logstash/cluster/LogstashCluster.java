package org.logstash.cluster;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.logstash.cluster.cluster.ClusterMetadata;
import org.logstash.cluster.cluster.ClusterService;
import org.logstash.cluster.cluster.ManagedClusterService;
import org.logstash.cluster.cluster.Node;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.cluster.impl.DefaultClusterService;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.messaging.ClusterEventService;
import org.logstash.cluster.messaging.ManagedClusterCommunicationService;
import org.logstash.cluster.messaging.ManagedClusterEventService;
import org.logstash.cluster.messaging.ManagedMessagingService;
import org.logstash.cluster.messaging.MessagingService;
import org.logstash.cluster.messaging.impl.DefaultClusterCommunicationService;
import org.logstash.cluster.messaging.impl.DefaultClusterEventService;
import org.logstash.cluster.messaging.netty.NettyMessagingService;
import org.logstash.cluster.partition.ManagedPartitionService;
import org.logstash.cluster.partition.PartitionId;
import org.logstash.cluster.partition.PartitionMetadata;
import org.logstash.cluster.partition.PartitionService;
import org.logstash.cluster.partition.impl.DefaultPartitionService;
import org.logstash.cluster.partition.impl.RaftPartition;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.PrimitiveService;
import org.logstash.cluster.primitives.counter.AtomicCounterBuilder;
import org.logstash.cluster.primitives.generator.AtomicIdGeneratorBuilder;
import org.logstash.cluster.primitives.impl.FederatedPrimitiveService;
import org.logstash.cluster.primitives.leadership.LeaderElectorBuilder;
import org.logstash.cluster.primitives.lock.DistributedLockBuilder;
import org.logstash.cluster.primitives.map.AtomicCounterMapBuilder;
import org.logstash.cluster.primitives.map.ConsistentMapBuilder;
import org.logstash.cluster.primitives.map.ConsistentTreeMapBuilder;
import org.logstash.cluster.primitives.multimap.ConsistentMultimapBuilder;
import org.logstash.cluster.primitives.queue.WorkQueueBuilder;
import org.logstash.cluster.primitives.set.DistributedSetBuilder;
import org.logstash.cluster.primitives.tree.DocumentTreeBuilder;
import org.logstash.cluster.primitives.value.AtomicValueBuilder;
import org.logstash.cluster.utils.Managed;
import org.logstash.cluster.utils.concurrent.SingleThreadContext;
import org.logstash.cluster.utils.concurrent.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class LogstashCluster implements PrimitiveService, Managed<LogstashCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogstashCluster.class);
    private final ManagedClusterService cluster;
    private final ManagedMessagingService messagingService;
    private final ManagedClusterCommunicationService clusterCommunicator;
    private final ManagedClusterEventService clusterEventService;
    private final ManagedPartitionService partitions;
    private final PrimitiveService primitives;
    private final AtomicBoolean open = new AtomicBoolean();
    private final ThreadContext context = new SingleThreadContext("atomix-%d");

    private LogstashCluster(final ManagedClusterService cluster,
        final ManagedMessagingService messagingService,
        final ManagedClusterCommunicationService clusterCommunicator,
        final ManagedClusterEventService clusterEventService,
        final ManagedPartitionService partitions, final PrimitiveService primitives) {
        this.cluster = Preconditions.checkNotNull(cluster, "cluster cannot be null");
        this.messagingService = Preconditions.checkNotNull(messagingService, "messagingService cannot be null");
        this.clusterCommunicator = Preconditions.checkNotNull(clusterCommunicator, "clusterCommunicator cannot be null");
        this.clusterEventService = Preconditions.checkNotNull(clusterEventService, "clusterEventService cannot be null");
        this.partitions = Preconditions.checkNotNull(partitions, "partitions cannot be null");
        this.primitives = Preconditions.checkNotNull(primitives, "primitives cannot be null");
    }

    public static LogstashCluster.Builder builder() {
        return new LogstashCluster.Builder();
    }

    public ClusterService getClusterService() {
        return cluster;
    }

    /**
     * Returns the cluster communicator.
     * @return the cluster communicator
     */
    public ClusterCommunicationService getCommunicationService() {
        return clusterCommunicator;
    }

    /**
     * Returns the cluster event service.
     * @return the cluster event service
     */
    public ClusterEventService getEventService() {
        return clusterEventService;
    }

    /**
     * Returns the cluster messenger.
     * @return the cluster messenger
     */
    public MessagingService getMessagingService() {
        return messagingService;
    }

    /**
     * Returns the primitive service.
     * @return the primitive service
     */
    public PrimitiveService getPrimitiveService() {
        return primitives;
    }

    @Override
    public <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder() {
        return primitives.consistentMapBuilder();
    }

    @Override
    public <V> DocumentTreeBuilder<V> documentTreeBuilder() {
        return primitives.documentTreeBuilder();
    }

    @Override
    public <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder() {
        return primitives.consistentTreeMapBuilder();
    }

    @Override
    public <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder() {
        return primitives.consistentMultimapBuilder();
    }

    @Override
    public <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder() {
        return primitives.atomicCounterMapBuilder();
    }

    @Override
    public <E> DistributedSetBuilder<E> setBuilder() {
        return primitives.setBuilder();
    }

    @Override
    public AtomicCounterBuilder atomicCounterBuilder() {
        return primitives.atomicCounterBuilder();
    }

    @Override
    public AtomicIdGeneratorBuilder atomicIdGeneratorBuilder() {
        return primitives.atomicIdGeneratorBuilder();
    }

    @Override
    public <V> AtomicValueBuilder<V> atomicValueBuilder() {
        return primitives.atomicValueBuilder();
    }

    @Override
    public <T> LeaderElectorBuilder<T> leaderElectorBuilder() {
        return primitives.leaderElectorBuilder();
    }

    @Override
    public DistributedLockBuilder lockBuilder() {
        return primitives.lockBuilder();
    }

    @Override
    public <E> WorkQueueBuilder<E> workQueueBuilder() {
        return primitives.workQueueBuilder();
    }

    @Override
    public Set<String> getPrimitiveNames(final DistributedPrimitive.Type primitiveType) {
        return primitives.getPrimitiveNames(primitiveType);
    }

    @Override
    public CompletableFuture<LogstashCluster> open() {
        return messagingService.open()
            .thenComposeAsync(v -> cluster.open(), context)
            .thenComposeAsync(v -> clusterCommunicator.open(), context)
            .thenComposeAsync(v -> clusterEventService.open(), context)
            .thenComposeAsync(v -> partitions.open(), context)
            .thenApplyAsync(v -> {
                open.set(true);
                LOGGER.info("Started");
                return this;
            }, context);
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public CompletableFuture<Void> close() {
        return partitions.close()
            .thenComposeAsync(v -> clusterCommunicator.close(), context)
            .thenComposeAsync(v -> clusterEventService.close(), context)
            .thenComposeAsync(v -> cluster.close(), context)
            .thenComposeAsync(v -> messagingService.close(), context)
            .thenRunAsync(() -> {
                context.close();
                open.set(false);
                LOGGER.info("Stopped");
            });
    }

    @Override
    public boolean isClosed() {
        return !open.get();
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("partitions", getPartitionService())
            .toString();
    }

    /**
     * Returns the partition service.
     * @return the partition service
     */
    public PartitionService getPartitionService() {
        return partitions;
    }

    public static class Builder implements org.logstash.cluster.utils.Builder<LogstashCluster> {
        private static final String DEFAULT_CLUSTER_NAME = "atomix";
        private static final int DEFAULT_NUM_BUCKETS = 128;
        private String name = DEFAULT_CLUSTER_NAME;
        private Node localNode;
        private Collection<Node> bootstrapNodes;
        private int numPartitions;
        private int partitionSize;
        private int numBuckets = DEFAULT_NUM_BUCKETS;
        private Collection<PartitionMetadata> partitions;
        private File dataDir = new File(System.getProperty("user.dir"), "data");

        /**
         * Sets the cluster name.
         * @param name the cluster name
         * @return the cluster metadata builder
         * @throws NullPointerException if the name is null
         */
        public LogstashCluster.Builder withClusterName(final String name) {
            this.name = Preconditions.checkNotNull(name, "name cannot be null");
            return this;
        }

        /**
         * Sets the local node metadata.
         * @param localNode the local node metadata
         * @return the cluster metadata builder
         */
        public LogstashCluster.Builder withLocalNode(final Node localNode) {
            this.localNode = Preconditions.checkNotNull(localNode, "localNode cannot be null");
            return this;
        }

        /**
         * Sets the bootstrap nodes.
         * @param bootstrapNodes the nodes from which to bootstrap the cluster
         * @return the cluster metadata builder
         * @throws NullPointerException if the bootstrap nodes are {@code null}
         */
        public LogstashCluster.Builder withBootstrapNodes(final Node... bootstrapNodes) {
            return withBootstrapNodes(Arrays.asList(Preconditions.checkNotNull(bootstrapNodes)));
        }

        /**
         * Sets the bootstrap nodes.
         * @param bootstrapNodes the nodes from which to bootstrap the cluster
         * @return the cluster metadata builder
         * @throws NullPointerException if the bootstrap nodes are {@code null}
         */
        public LogstashCluster.Builder withBootstrapNodes(final Collection<Node> bootstrapNodes) {
            this.bootstrapNodes = Preconditions.checkNotNull(bootstrapNodes, "bootstrapNodes cannot be null");
            return this;
        }

        /**
         * Sets the number of partitions.
         * @param numPartitions the number of partitions
         * @return the cluster metadata builder
         * @throws IllegalArgumentException if the number of partitions is not positive
         */
        public LogstashCluster.Builder withNumPartitions(final int numPartitions) {
            Preconditions.checkArgument(numPartitions > 0, "numPartitions must be positive");
            this.numPartitions = numPartitions;
            return this;
        }

        /**
         * Sets the partition size.
         * @param partitionSize the partition size
         * @return the cluster metadata builder
         * @throws IllegalArgumentException if the partition size is not positive
         */
        public LogstashCluster.Builder withPartitionSize(final int partitionSize) {
            Preconditions.checkArgument(partitionSize > 0, "partitionSize must be positive");
            this.partitionSize = partitionSize;
            return this;
        }

        /**
         * Sets the number of buckets within each partition.
         * @param numBuckets the number of buckets within each partition
         * @return the cluster metadata builder
         * @throws IllegalArgumentException if the number of buckets within each partition is not positive
         */
        public LogstashCluster.Builder withNumBuckets(final int numBuckets) {
            Preconditions.checkArgument(numBuckets > 0, "numBuckets must be positive");
            this.numBuckets = numBuckets;
            return this;
        }

        /**
         * Sets the partitions.
         * @param partitions the partitions
         * @return the cluster metadata builder
         */
        public LogstashCluster.Builder withPartitions(final Collection<PartitionMetadata> partitions) {
            this.partitions = Preconditions.checkNotNull(partitions, "partitions cannot be null");
            return this;
        }

        /**
         * Sets the path to the data directory.
         * @param dataDir the path to the replica's data directory
         * @return the replica builder
         */
        public LogstashCluster.Builder withDataDir(final File dataDir) {
            this.dataDir = Preconditions.checkNotNull(dataDir, "dataDir cannot be null");
            return this;
        }

        @Override
        public LogstashCluster build() {
            final ManagedMessagingService messagingService = buildMessagingService();
            final ManagedClusterService clusterService = buildClusterService(messagingService);
            final ManagedClusterCommunicationService clusterCommunicator = buildClusterCommunicationService(clusterService, messagingService);
            final ManagedClusterEventService clusterEventService = buildClusterEventService(clusterService, clusterCommunicator);
            final ManagedPartitionService partitionService = buildPartitionService(clusterCommunicator);
            final PrimitiveService primitives = buildPrimitiveService(partitionService);
            return new LogstashCluster(
                clusterService,
                messagingService,
                clusterCommunicator,
                clusterEventService,
                partitionService,
                primitives);
        }

        /**
         * Builds a default messaging service.
         */
        private ManagedMessagingService buildMessagingService() {
            return NettyMessagingService.builder()
                .withName(name)
                .withEndpoint(localNode.endpoint())
                .build();
        }

        /**
         * Builds a cluster service.
         */
        private ManagedClusterService buildClusterService(final MessagingService messagingService) {
            return new DefaultClusterService(ClusterMetadata.builder()
                .withLocalNode(localNode)
                .withBootstrapNodes(bootstrapNodes)
                .build(), messagingService);
        }

        /**
         * Builds a cluster communication service.
         */
        private static ManagedClusterCommunicationService buildClusterCommunicationService(
            final ClusterService clusterService, final MessagingService messagingService) {
            return new DefaultClusterCommunicationService(clusterService, messagingService);
        }

        /**
         * Builds a cluster event service.
         */
        private static ManagedClusterEventService buildClusterEventService(
            final ClusterService clusterService, final ClusterCommunicationService clusterCommunicator) {
            return new DefaultClusterEventService(clusterService, clusterCommunicator);
        }

        /**
         * Builds a partition service.
         */
        private ManagedPartitionService buildPartitionService(final ClusterCommunicationService clusterCommunicator) {
            final File partitionsDir = new File(this.dataDir, "partitions");
            final Collection<RaftPartition> partitions = buildPartitions().stream()
                .map(p -> new RaftPartition(localNode.id(), p, clusterCommunicator, new File(partitionsDir, p.id().toString())))
                .collect(Collectors.toList());
            return new DefaultPartitionService(partitions);
        }

        /**
         * Builds the cluster partitions.
         */
        private Collection<PartitionMetadata> buildPartitions() {
            if (partitions != null) {
                return partitions;
            }

            if (numPartitions == 0) {
                numPartitions = bootstrapNodes.size();
            }

            if (partitionSize == 0) {
                partitionSize = Math.min(bootstrapNodes.size(), 3);
            }

            final List<Node> sorted = new ArrayList<>(bootstrapNodes);
            sorted.sort(Comparator.comparing(Node::id));

            final Set<PartitionMetadata> partitions = Sets.newHashSet();
            for (int i = 0; i < numPartitions; i++) {
                final Set<NodeId> set = new HashSet<>(partitionSize);
                for (int j = 0; j < partitionSize; j++) {
                    set.add(sorted.get((i + j) % numPartitions).id());
                }
                partitions.add(new PartitionMetadata(PartitionId.from(i + 1), set));
            }
            return partitions;
        }

        /**
         * Builds a primitive service.
         */
        private PrimitiveService buildPrimitiveService(final PartitionService partitionService) {
            final Map<Integer, DistributedPrimitiveCreator> members = new HashMap<>();
            partitionService.getPartitions().forEach(p -> members.put(p.id().id(), partitionService.getPrimitiveCreator(p.id())));
            return new FederatedPrimitiveService(members, numBuckets);
        }

    }
}

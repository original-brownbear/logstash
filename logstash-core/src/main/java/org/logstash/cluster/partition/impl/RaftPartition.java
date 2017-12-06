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
package org.logstash.cluster.partition.impl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.partition.ManagedPartition;
import org.logstash.cluster.partition.Partition;
import org.logstash.cluster.partition.PartitionId;
import org.logstash.cluster.partition.PartitionMetadata;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.counter.AtomicCounterBuilder;
import org.logstash.cluster.primitives.counter.impl.DefaultAtomicCounterBuilder;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounterService;
import org.logstash.cluster.primitives.generator.AtomicIdGeneratorBuilder;
import org.logstash.cluster.primitives.generator.impl.DefaultAtomicIdGeneratorBuilder;
import org.logstash.cluster.primitives.leadership.LeaderElectorBuilder;
import org.logstash.cluster.primitives.leadership.impl.DefaultLeaderElectorBuilder;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElectorService;
import org.logstash.cluster.primitives.lock.DistributedLockBuilder;
import org.logstash.cluster.primitives.lock.impl.DefaultDistributedLockBuilder;
import org.logstash.cluster.primitives.lock.impl.RaftDistributedLockService;
import org.logstash.cluster.primitives.map.AtomicCounterMapBuilder;
import org.logstash.cluster.primitives.map.ConsistentMapBuilder;
import org.logstash.cluster.primitives.map.ConsistentTreeMapBuilder;
import org.logstash.cluster.primitives.map.impl.DefaultAtomicCounterMapBuilder;
import org.logstash.cluster.primitives.map.impl.DefaultConsistentMapBuilder;
import org.logstash.cluster.primitives.map.impl.DefaultConsistentTreeMapBuilder;
import org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMapService;
import org.logstash.cluster.primitives.map.impl.RaftConsistentMapService;
import org.logstash.cluster.primitives.map.impl.RaftConsistentTreeMapService;
import org.logstash.cluster.primitives.multimap.ConsistentMultimapBuilder;
import org.logstash.cluster.primitives.multimap.impl.DefaultConsistentMultimapBuilder;
import org.logstash.cluster.primitives.multimap.impl.RaftConsistentSetMultimapService;
import org.logstash.cluster.primitives.queue.WorkQueueBuilder;
import org.logstash.cluster.primitives.queue.impl.DefaultWorkQueueBuilder;
import org.logstash.cluster.primitives.queue.impl.RaftWorkQueueService;
import org.logstash.cluster.primitives.set.DistributedSetBuilder;
import org.logstash.cluster.primitives.set.impl.DefaultDistributedSetBuilder;
import org.logstash.cluster.primitives.tree.DocumentTreeBuilder;
import org.logstash.cluster.primitives.tree.impl.DefaultDocumentTreeBuilder;
import org.logstash.cluster.primitives.tree.impl.RaftDocumentTreeService;
import org.logstash.cluster.primitives.value.AtomicValueBuilder;
import org.logstash.cluster.primitives.value.impl.DefaultAtomicValueBuilder;
import org.logstash.cluster.primitives.value.impl.RaftAtomicValueService;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.service.RaftService;
import org.logstash.cluster.serializer.Serializer;

/**
 * Abstract partition.
 */
public class RaftPartition implements ManagedPartition {
    public static final Map<String, Supplier<RaftService>> RAFT_SERVICES =
        ImmutableMap.<String, Supplier<RaftService>>builder()
            .put(DistributedPrimitive.Type.CONSISTENT_MAP.name(), RaftConsistentMapService::new)
            .put(DistributedPrimitive.Type.CONSISTENT_TREEMAP.name(), RaftConsistentTreeMapService::new)
            .put(DistributedPrimitive.Type.CONSISTENT_MULTIMAP.name(), RaftConsistentSetMultimapService::new)
            .put(DistributedPrimitive.Type.COUNTER_MAP.name(), RaftAtomicCounterMapService::new)
            .put(DistributedPrimitive.Type.COUNTER.name(), RaftAtomicCounterService::new)
            .put(DistributedPrimitive.Type.LEADER_ELECTOR.name(), RaftLeaderElectorService::new)
            .put(DistributedPrimitive.Type.LOCK.name(), RaftDistributedLockService::new)
            .put(DistributedPrimitive.Type.WORK_QUEUE.name(), RaftWorkQueueService::new)
            .put(DistributedPrimitive.Type.VALUE.name(), RaftAtomicValueService::new)
            .put(DistributedPrimitive.Type.DOCUMENT_TREE.name(),
                () -> new RaftDocumentTreeService(Ordering.NATURAL))
            .put(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), Ordering.NATURAL),
                () -> new RaftDocumentTreeService(Ordering.NATURAL))
            .put(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), Ordering.INSERTION),
                () -> new RaftDocumentTreeService(Ordering.INSERTION))
            .build();
    protected final AtomicBoolean isOpened = new AtomicBoolean(false);
    protected final ClusterCommunicationService clusterCommunicator;
    protected final PartitionMetadata partition;
    protected final NodeId localNodeId;
    private final File dataDir;
    private final RaftPartitionClient client;
    private final RaftPartitionServer server;

    public RaftPartition(
        NodeId nodeId,
        PartitionMetadata partition,
        ClusterCommunicationService clusterCommunicator,
        File dataDir) {
        this.localNodeId = nodeId;
        this.partition = partition;
        this.clusterCommunicator = clusterCommunicator;
        this.dataDir = dataDir;
        this.client = createClient();
        this.server = createServer();
    }

    /**
     * Creates a Raft server.
     */
    protected RaftPartitionServer createServer() {
        return new RaftPartitionServer(
            this,
            MemberId.from(localNodeId.id()),
            clusterCommunicator);
    }

    /**
     * Creates a Raft client.
     */
    private RaftPartitionClient createClient() {
        return new RaftPartitionClient(
            this,
            MemberId.from(localNodeId.id()),
            new RaftClientCommunicator(
                name(),
                Serializer.using(RaftNamespaces.RAFT_PROTOCOL),
                clusterCommunicator));
    }

    /**
     * Returns the partition primitive creator.
     * @return the partition primitive creator
     */
    public DistributedPrimitiveCreator getPrimitiveCreator() {
        return client;
    }

    /**
     * Returns the partition name.
     * @return the partition name
     */
    public String name() {
        return String.format("partition-%d", partition.id().id());
    }

    /**
     * Returns the {@link MemberId identifiers} of partition members.
     * @return partition member identifiers
     */
    Collection<MemberId> getMemberIds() {
        return Collections2.transform(members(), n -> MemberId.from(n.id()));
    }

    /**
     * Returns the identifiers of partition members.
     * @return partition member instance ids
     */
    Collection<NodeId> members() {
        return partition.members();
    }

    /**
     * Returns the partition data directory.
     * @return the partition data directory
     */
    File getDataDir() {
        return dataDir;
    }

    @Override
    public CompletableFuture<Partition> open() {
        if (partition.members().contains(localNodeId)) {
            return server.open()
                .thenCompose(v -> client.open())
                .thenAccept(v -> isOpened.set(true))
                .thenApply(v -> null);
        }
        return client.open()
            .thenAccept(v -> isOpened.set(true))
            .thenApply(v -> this);
    }

    @Override
    public <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder() {
        return new DefaultConsistentMapBuilder<>(getPrimitiveCreator());
    }

    @Override
    public boolean isOpen() {
        return isOpened.get();
    }

    @Override
    public <V> DocumentTreeBuilder<V> documentTreeBuilder() {
        return new DefaultDocumentTreeBuilder<>(getPrimitiveCreator());
    }

    @Override
    public CompletableFuture<Void> close() {
        // We do not explicitly close the server and instead let the cluster
        // deal with this as an unclean exit.
        return client.close();
    }

    @Override
    public <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder() {
        return new DefaultConsistentTreeMapBuilder<>(getPrimitiveCreator());
    }

    @Override
    public boolean isClosed() {
        return !isOpened.get();
    }

    @Override
    public <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder() {
        return new DefaultConsistentMultimapBuilder<>(getPrimitiveCreator());
    }

    /**
     * Deletes the partition.
     * @return future to be completed once the partition has been deleted
     */
    public CompletableFuture<Void> delete() {
        return server.close().thenCompose(v -> client.close()).thenRun(() -> server.delete());
    }

    @Override
    public <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder() {
        return new DefaultAtomicCounterMapBuilder<>(getPrimitiveCreator());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("partitionId", id())
            .toString();
    }

    @Override
    public <E> DistributedSetBuilder<E> setBuilder() {
        return new DefaultDistributedSetBuilder<>(() -> consistentMapBuilder());
    }

    @Override
    public PartitionId id() {
        return partition.id();
    }

    @Override
    public AtomicCounterBuilder atomicCounterBuilder() {
        return new DefaultAtomicCounterBuilder(getPrimitiveCreator());
    }

    @Override
    public AtomicIdGeneratorBuilder atomicIdGeneratorBuilder() {
        return new DefaultAtomicIdGeneratorBuilder(getPrimitiveCreator());
    }

    @Override
    public <V> AtomicValueBuilder<V> atomicValueBuilder() {
        return new DefaultAtomicValueBuilder<>(getPrimitiveCreator());
    }

    @Override
    public <T> LeaderElectorBuilder<T> leaderElectorBuilder() {
        return new DefaultLeaderElectorBuilder<>(getPrimitiveCreator());
    }

    @Override
    public <E> WorkQueueBuilder<E> workQueueBuilder() {
        return new DefaultWorkQueueBuilder<>(getPrimitiveCreator());
    }

    @Override
    public DistributedLockBuilder lockBuilder() {
        return new DefaultDistributedLockBuilder(getPrimitiveCreator());
    }

    @Override
    public Set<String> getPrimitiveNames(DistributedPrimitive.Type primitiveType) {
        return getPrimitiveCreator().getPrimitiveNames(primitiveType);
    }

}

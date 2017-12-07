package org.logstash.cluster.partition.impl;

import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.logstash.cluster.partition.ManagedPartition;
import org.logstash.cluster.partition.ManagedPartitionService;
import org.logstash.cluster.partition.Partition;
import org.logstash.cluster.partition.PartitionId;
import org.logstash.cluster.partition.PartitionService;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default partition service.
 */
public class DefaultPartitionService implements ManagedPartitionService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPartitionService.class);

    private final TreeMap<PartitionId, RaftPartition> partitions = new TreeMap<>();
    private final AtomicBoolean open = new AtomicBoolean();

    public DefaultPartitionService(Collection<RaftPartition> partitions) {
        partitions.forEach(p -> this.partitions.put(p.id(), p));
    }

    @Override
    public Partition getPartition(PartitionId partitionId) {
        return partitions.get(partitionId);
    }

    @Override
    public DistributedPrimitiveCreator getPrimitiveCreator(PartitionId partitionId) {
        return partitions.get(partitionId).getPrimitiveCreator();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Partition> getPartitions() {
        return (Collection) partitions.values();
    }

    @Override
    public CompletableFuture<PartitionService> open() {
        List<CompletableFuture<Partition>> futures = partitions.values().stream()
            .map(ManagedPartition::open)
            .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenApply(v -> {
            open.set(true);
            LOGGER.info("Started");
            return this;
        });
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public CompletableFuture<Void> close() {
        List<CompletableFuture<Void>> futures = partitions.values().stream()
            .map(ManagedPartition::close)
            .collect(Collectors.toList());
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).thenRun(() -> {
            open.set(false);
            LOGGER.info("Stopped");
        });
    }

    @Override
    public boolean isClosed() {
        return !open.get();
    }
}
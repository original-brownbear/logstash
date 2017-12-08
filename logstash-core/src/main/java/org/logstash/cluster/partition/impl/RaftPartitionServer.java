package org.logstash.cluster.partition.impl;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.utils.Managed;

/**
 * {@link org.logstash.cluster.partition.Partition} server.
 */
public class RaftPartitionServer implements Managed<RaftPartitionServer> {

    private static final Logger LOGGER = LogManager.getLogger(RaftPartitionServer.class);

    private static final int MAX_SEGMENT_SIZE = 1024 * 1024 * 64;
    private static final long ELECTION_TIMEOUT_MILLIS = 2500;
    private static final long HEARTBEAT_INTERVAL_MILLIS = 250;
    private final MemberId localMemberId;
    private final RaftPartition partition;
    private final ClusterCommunicationService clusterCommunicator;
    private RaftServer server;

    public RaftPartitionServer(
        RaftPartition partition,
        MemberId localMemberId,
        ClusterCommunicationService clusterCommunicator) {
        this.partition = partition;
        this.localMemberId = localMemberId;
        this.clusterCommunicator = clusterCommunicator;
    }

    @Override
    public CompletableFuture<RaftPartitionServer> open() {
        LOGGER.info("Starting server for partition {}", partition.id());
        CompletableFuture<RaftServer> serverOpenFuture;
        if (partition.getMemberIds().contains(localMemberId)) {
            if (server != null && server.isRunning()) {
                return CompletableFuture.completedFuture(null);
            }
            synchronized (this) {
                server = buildServer();
            }
            serverOpenFuture = server.bootstrap(partition.getMemberIds());
        } else {
            serverOpenFuture = CompletableFuture.completedFuture(null);
        }
        return serverOpenFuture.whenComplete((r, e) -> {
            if (e == null) {
                LOGGER.info("Successfully started server for partition {}", partition.id());
            } else {
                LOGGER.info("Failed to start server for partition {}", partition.id(), e);
            }
        }).thenApply(v -> this);
    }

    private RaftServer buildServer() {
        RaftServer.Builder builder = RaftServer.builder(localMemberId)
            .withName(partition.name())
            .withProtocol(new RaftServerCommunicator(
                partition.name(),
                Serializer.using(RaftNamespaces.RAFT_PROTOCOL),
                clusterCommunicator))
            .withElectionTimeout(Duration.ofMillis(ELECTION_TIMEOUT_MILLIS))
            .withHeartbeatInterval(Duration.ofMillis(HEARTBEAT_INTERVAL_MILLIS))
            .withStorage(RaftStorage.builder()
                .withPrefix(String.format("partition-%s", partition.id()))
                .withStorageLevel(StorageLevel.MAPPED)
                .withSerializer(Serializer.using(RaftNamespaces.RAFT_STORAGE))
                .withDirectory(partition.getDataDir())
                .withMaxSegmentSize(MAX_SEGMENT_SIZE)
                .build());
        RaftPartition.RAFT_SERVICES.forEach(builder::addService);
        return builder.build();
    }

    @Override
    public boolean isOpen() {
        return server.isRunning();
    }

    @Override
    public CompletableFuture<Void> close() {
        return server.shutdown();
    }

    @Override
    public boolean isClosed() {
        return !isOpen();
    }

    /**
     * Closes the server and exits the partition.
     * @return future that is completed when the operation is complete
     */
    public CompletableFuture<Void> closeAndExit() {
        return server.leave();
    }

    /**
     * Deletes the server.
     */
    public void delete() {
        try {
            Files.walkFileTree(partition.getDataDir().toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to delete partition: {}", e);
        }
    }

    public CompletableFuture<Void> join(Collection<MemberId> otherMembers) {
        LOGGER.info("Joining partition {} ({})", partition.id(), partition.name());
        server = buildServer();
        return server.join(otherMembers).whenComplete((r, e) -> {
            if (e == null) {
                LOGGER.info("Successfully joined partition {} ({})", partition.id(), partition.name());
            } else {
                LOGGER.info("Failed to join partition {} ({})", partition.id(), partition.name(), e);
            }
        }).thenApply(v -> null);
    }
}

/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.logstash.cluster.messaging.ClusterCommunicationService;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.storage.StorageLevel;
import org.logstash.cluster.utils.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Partition} server.
 */
public class RaftPartitionServer implements Managed<RaftPartitionServer> {

    private static final int MAX_SEGMENT_SIZE = 1024 * 1024 * 64;
    private static final long ELECTION_TIMEOUT_MILLIS = 2500;
    private static final long HEARTBEAT_INTERVAL_MILLIS = 250;
    private final Logger log = LoggerFactory.getLogger(getClass());
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
        log.info("Starting server for partition {}", partition.id());
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
                log.info("Successfully started server for partition {}", partition.id());
            } else {
                log.info("Failed to start server for partition {}", partition.id(), e);
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
            log.error("Failed to delete partition: {}", e);
        }
    }

    public CompletableFuture<Void> join(Collection<MemberId> otherMembers) {
        log.info("Joining partition {} ({})", partition.id(), partition.name());
        server = buildServer();
        return server.join(otherMembers).whenComplete((r, e) -> {
            if (e == null) {
                log.info("Successfully joined partition {} ({})", partition.id(), partition.name());
            } else {
                log.info("Failed to join partition {} ({})", partition.id(), partition.name(), e);
            }
        }).thenApply(v -> null);
    }
}

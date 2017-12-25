package org.logstash.cluster.primitives.impl;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.messaging.TestClusterCommunicationServiceFactory;
import org.logstash.cluster.messaging.TestRaftClientCommunicator;
import org.logstash.cluster.messaging.TestRaftServerCommunicator;
import org.logstash.cluster.protocols.raft.RaftClient;
import org.logstash.cluster.protocols.raft.RaftServer;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.cluster.RaftMember;
import org.logstash.cluster.protocols.raft.proxy.CommunicationStrategy;
import org.logstash.cluster.protocols.raft.proxy.RaftProxy;
import org.logstash.cluster.protocols.raft.service.RaftService;
import org.logstash.cluster.protocols.raft.storage.RaftStorage;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.storage.StorageLevel;

/**
 * Base class for various tests.
 * @param <T> the Raft primitive type being tested
 */
public abstract class AbstractRaftPrimitiveTest<T extends AbstractRaftPrimitive> {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Path basePath;

    private TestClusterCommunicationServiceFactory communicationServiceFactory;
    private List<RaftMember> members = Lists.newCopyOnWriteArrayList();
    private List<RaftClient> clients = Lists.newCopyOnWriteArrayList();
    private List<RaftServer> servers = Lists.newCopyOnWriteArrayList();
    private int nextId;

    /**
     * Creates the primitive service.
     * @return the primitive service
     */
    protected abstract RaftService createService();

    /**
     * Creates a new primitive.
     * @param name the primitive name
     * @return the primitive instance
     */
    protected T newPrimitive(final String name) {
        final RaftClient client = createClient();
        final RaftProxy proxy = client.newProxyBuilder()
            .withName(name)
            .withServiceType("test")
            .withReadConsistency(readConsistency())
            .withCommunicationStrategy(communicationStrategy())
            .build()
            .open()
            .join();
        return createPrimitive(proxy);
    }

    /**
     * Creates a new primitive instance.
     * @param proxy the primitive proxy
     * @return the primitive instance
     */
    protected abstract T createPrimitive(RaftProxy proxy);

    /**
     * Returns the proxy read consistency.
     * @return the primitive read consistency
     */
    private ReadConsistency readConsistency() {
        return ReadConsistency.LINEARIZABLE;
    }

    /**
     * Returns the proxy communication strategy.
     * @return the primitive communication strategy
     */
    private CommunicationStrategy communicationStrategy() {
        return CommunicationStrategy.LEADER;
    }

    /**
     * Creates a Raft client.
     */
    private RaftClient createClient() {
        final MemberId memberId = nextMemberId();
        final RaftClient client = RaftClient.builder()
            .withMemberId(memberId)
            .withProtocol(new TestRaftClientCommunicator(
                "partition-1",
                Serializer.using(RaftTestNamespaces.RAFT_PROTOCOL),
                communicationServiceFactory.newCommunicationService(NodeId.from(memberId.id()))))
            .build();

        client.connect(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).join();
        clients.add(client);
        return client;
    }

    /**
     * Returns the next unique member identifier.
     * @return The next unique member identifier.
     */
    private MemberId nextMemberId() {
        return MemberId.from(String.valueOf(++nextId));
    }

    @Before
    public void prepare() throws IOException {
        basePath = temporaryFolder.newFolder().toPath();
        members.clear();
        clients.clear();
        servers.clear();
        communicationServiceFactory = new TestClusterCommunicationServiceFactory();
        createServers(3);
    }

    /**
     * Creates a set of Raft servers.
     */
    private List<RaftServer> createServers(final int nodes) {
        final List<RaftServer> servers = new ArrayList<>();

        for (int i = 0; i < nodes; i++) {
            members.add(nextMember(RaftMember.Type.ACTIVE));
        }

        final CountDownLatch latch = new CountDownLatch(nodes);
        for (int i = 0; i < nodes; i++) {
            final RaftServer server = createServer(members.get(i));
            server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList()))
                .thenRun(latch::countDown);
            servers.add(server);
        }

        try {
            latch.await(30000, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        return servers;
    }

    /**
     * Returns the next server address.
     * @param type The startup member type.
     * @return The next server address.
     */
    private RaftMember nextMember(final RaftMember.Type type) {
        return new AbstractRaftPrimitiveTest.TestMember(nextMemberId(), type);
    }

    /**
     * Creates a Raft server.
     */
    private RaftServer createServer(final RaftMember member) {
        final RaftServer.Builder builder = RaftServer.builder(member.memberId())
            .withProtocol(new TestRaftServerCommunicator(
                "partition-1",
                Serializer.using(RaftTestNamespaces.RAFT_PROTOCOL),
                communicationServiceFactory.newCommunicationService(NodeId.from(member.memberId().id()))))
            .withStorage(RaftStorage.builder()
                .withStorageLevel(StorageLevel.MEMORY)
                .withDirectory(basePath.resolve("primitives").resolve(member.memberId().id()).toFile())
                .withSerializer(Serializer.using(RaftTestNamespaces.RAFT_STORAGE))
                .withMaxSegmentSize(1024 * 1024)
                .build())
            .addService("test", this::createService);

        final RaftServer server = builder.build();
        servers.add(server);
        return server;
    }

    @After
    public void cleanup() {
        shutdown();
    }

    /**
     * Shuts down clients and servers.
     */
    private void shutdown() {
        clients.forEach(c -> {
            try {
                c.close().get(10, TimeUnit.SECONDS);
            } catch (final Exception e) {
            }
        });

        servers.forEach(s -> {
            try {
                if (s.isRunning()) {
                    s.shutdown().get(10, TimeUnit.SECONDS);
                }
            } catch (final Exception e) {
            }
        });

        final Path directory = basePath.resolve("primitives");
        if (Files.exists(directory)) {
            try {
                Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (final IOException e) {
            }
        }
    }

    /**
     * Test member.
     */
    public static class TestMember implements RaftMember {
        private final MemberId memberId;
        private final RaftMember.Type type;

        TestMember(final MemberId memberId, final RaftMember.Type type) {
            this.memberId = memberId;
            this.type = type;
        }

        @Override
        public MemberId memberId() {
            return memberId;
        }

        @Override
        public int hash() {
            return memberId.hashCode();
        }

        @Override
        public RaftMember.Type getType() {
            return type;
        }

        @Override
        public void addTypeChangeListener(final Consumer<RaftMember.Type> listener) {

        }

        @Override
        public void removeTypeChangeListener(final Consumer<RaftMember.Type> listener) {

        }

        @Override
        public Instant getLastUpdated() {
            return Instant.now();
        }

        @Override
        public CompletableFuture<Void> promote() {
            return null;
        }

        @Override
        public CompletableFuture<Void> promote(final RaftMember.Type type) {
            return null;
        }

        @Override
        public CompletableFuture<Void> demote() {
            return null;
        }

        @Override
        public CompletableFuture<Void> demote(final RaftMember.Type type) {
            return null;
        }

        @Override
        public CompletableFuture<Void> remove() {
            return null;
        }
    }
}

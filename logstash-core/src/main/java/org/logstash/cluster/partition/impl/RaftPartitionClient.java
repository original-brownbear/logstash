package org.logstash.cluster.partition.impl;

import com.google.common.io.BaseEncoding;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.DistributedPrimitives;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.counter.AsyncAtomicCounter;
import org.logstash.cluster.primitives.counter.impl.RaftAtomicCounter;
import org.logstash.cluster.primitives.generator.AsyncAtomicIdGenerator;
import org.logstash.cluster.primitives.generator.impl.RaftIdGenerator;
import org.logstash.cluster.primitives.leadership.AsyncLeaderElector;
import org.logstash.cluster.primitives.leadership.impl.RaftLeaderElector;
import org.logstash.cluster.primitives.leadership.impl.TranscodingAsyncLeaderElector;
import org.logstash.cluster.primitives.lock.AsyncDistributedLock;
import org.logstash.cluster.primitives.lock.impl.RaftDistributedLock;
import org.logstash.cluster.primitives.map.AsyncAtomicCounterMap;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.primitives.map.AsyncConsistentTreeMap;
import org.logstash.cluster.primitives.map.impl.RaftAtomicCounterMap;
import org.logstash.cluster.primitives.map.impl.RaftConsistentMap;
import org.logstash.cluster.primitives.map.impl.RaftConsistentTreeMap;
import org.logstash.cluster.primitives.multimap.AsyncConsistentMultimap;
import org.logstash.cluster.primitives.multimap.impl.RaftConsistentSetMultimap;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.queue.impl.RaftWorkQueue;
import org.logstash.cluster.primitives.queue.impl.TranscodingAsyncWorkQueue;
import org.logstash.cluster.primitives.set.AsyncDistributedSet;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.impl.RaftDocumentTree;
import org.logstash.cluster.primitives.tree.impl.TranscodingAsyncDocumentTree;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.primitives.value.impl.RaftAtomicValue;
import org.logstash.cluster.primitives.value.impl.TranscodingAsyncAtomicValue;
import org.logstash.cluster.protocols.raft.RaftClient;
import org.logstash.cluster.protocols.raft.ReadConsistency;
import org.logstash.cluster.protocols.raft.cluster.MemberId;
import org.logstash.cluster.protocols.raft.protocol.RaftClientProtocol;
import org.logstash.cluster.protocols.raft.proxy.CommunicationStrategy;
import org.logstash.cluster.protocols.raft.session.RaftSessionMetadata;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.utils.Managed;

/**
 * StoragePartition client.
 */
public class RaftPartitionClient implements DistributedPrimitiveCreator, Managed<RaftPartitionClient> {

    private static final Logger LOGGER = LogManager.getLogger(RaftPartitionClient.class);

    private final RaftPartition partition;
    private final MemberId localMemberId;
    private final RaftClientProtocol protocol;
    private RaftClient client;

    public RaftPartitionClient(RaftPartition partition, MemberId localMemberId, RaftClientProtocol protocol) {
        this.partition = partition;
        this.localMemberId = localMemberId;
        this.protocol = protocol;
    }

    @Override
    public CompletableFuture<RaftPartitionClient> open() {
        synchronized (this) {
            client = newRaftClient(protocol);
        }
        return client.connect(partition.getMemberIds()).whenComplete((r, e) -> {
            if (e == null) {
                LOGGER.info("Successfully started client for partition {}", partition.id());
            } else {
                LOGGER.info("Failed to start client for partition {}", partition.id(), e);
            }
        }).thenApply(v -> null);
    }

    @Override
    public boolean isOpen() {
        return client != null;
    }

    @Override
    public CompletableFuture<Void> close() {
        return client != null ? client.close() : CompletableFuture.completedFuture(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(String name, Serializer serializer) {
        RaftConsistentMap rawMap = new RaftConsistentMap(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.CONSISTENT_MAP.name())
            .withReadConsistency(ReadConsistency.SEQUENTIAL)
            .withCommunicationStrategy(CommunicationStrategy.ANY)
            .withTimeout(Duration.ofSeconds(30))
            .withMaxRetries(5)
            .build()
            .open()
            .join());

        if (serializer != null) {
            return DistributedPrimitives.newTranscodingMap(rawMap,
                key -> BaseEncoding.base16().encode(serializer.encode(key)),
                string -> serializer.decode(BaseEncoding.base16().decode(string)),
                value -> value == null ? null : serializer.encode(value),
                bytes -> serializer.decode(bytes));
        }
        return (AsyncConsistentMap<K, V>) rawMap;
    }

    @Override
    public boolean isClosed() {
        return client == null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> AsyncConsistentTreeMap<K, V> newAsyncConsistentTreeMap(String name, Serializer serializer) {
        RaftConsistentTreeMap rawMap =
            new RaftConsistentTreeMap(client.newProxyBuilder()
                .withName(name)
                .withServiceType(DistributedPrimitive.Type.CONSISTENT_TREEMAP.name())
                .withReadConsistency(ReadConsistency.SEQUENTIAL)
                .withCommunicationStrategy(CommunicationStrategy.ANY)
                .withTimeout(Duration.ofSeconds(30))
                .withMaxRetries(5)
                .build()
                .open()
                .join());

        if (serializer != null) {
            return DistributedPrimitives.newTranscodingTreeMap(
                rawMap,
                key -> BaseEncoding.base16().encode(serializer.encode(key)),
                string -> serializer.decode(BaseEncoding.base16().decode(string)),
                value -> value == null ? null : serializer.encode(value),
                bytes -> serializer.decode(bytes));
        }
        return (AsyncConsistentTreeMap<K, V>) rawMap;
    }

    private RaftClient newRaftClient(RaftClientProtocol protocol) {
        return RaftClient.builder()
            .withClientId(partition.name())
            .withMemberId(localMemberId)
            .withProtocol(protocol)
            .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(String name, Serializer serializer) {
        RaftConsistentSetMultimap rawMap =
            new RaftConsistentSetMultimap(client.newProxyBuilder()
                .withName(name)
                .withServiceType(DistributedPrimitive.Type.CONSISTENT_MULTIMAP.name())
                .withReadConsistency(ReadConsistency.SEQUENTIAL)
                .withCommunicationStrategy(CommunicationStrategy.ANY)
                .withTimeout(Duration.ofSeconds(30))
                .withMaxRetries(5)
                .build()
                .open()
                .join());

        if (serializer != null) {
            return DistributedPrimitives.newTranscodingMultimap(
                rawMap,
                key -> BaseEncoding.base16().encode(serializer.encode(key)),
                string -> serializer.decode(BaseEncoding.base16().decode(string)),
                value -> serializer.encode(value),
                bytes -> serializer.decode(bytes));
        }
        return (AsyncConsistentMultimap<K, V>) rawMap;
    }

    @Override
    public <E> AsyncDistributedSet<E> newAsyncDistributedSet(String name, Serializer serializer) {
        return DistributedPrimitives.newSetFromMap(newAsyncConsistentMap(name, serializer));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(String name, Serializer serializer) {
        RaftAtomicCounterMap rawMap = new RaftAtomicCounterMap(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.COUNTER_MAP.name())
            .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withTimeout(Duration.ofSeconds(30))
            .withMaxRetries(5)
            .build()
            .open()
            .join());

        if (serializer != null) {
            return DistributedPrimitives.newTranscodingAtomicCounterMap(
                rawMap,
                key -> BaseEncoding.base16().encode(serializer.encode(key)),
                string -> serializer.decode(BaseEncoding.base16().decode(string)));
        }
        return (AsyncAtomicCounterMap<K>) rawMap;
    }

    @Override
    public AsyncAtomicCounter newAsyncCounter(String name) {
        return new RaftAtomicCounter(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.COUNTER.name())
            .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withTimeout(Duration.ofSeconds(30))
            .withMaxRetries(5)
            .build()
            .open()
            .join());
    }

    @Override
    public AsyncAtomicIdGenerator newAsyncIdGenerator(String name) {
        return new RaftIdGenerator(newAsyncCounter(name));
    }

    @Override
    public <V> AsyncAtomicValue<V> newAsyncAtomicValue(String name, Serializer serializer) {
        RaftAtomicValue value = new RaftAtomicValue(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.VALUE.name())
            .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
            .withCommunicationStrategy(CommunicationStrategy.ANY)
            .withTimeout(Duration.ofSeconds(30))
            .withMaxRetries(5)
            .build()
            .open()
            .join());
        return new TranscodingAsyncAtomicValue<>(value, serializer::encode, serializer::decode);
    }

    @Override
    public <E> AsyncWorkQueue<E> newAsyncWorkQueue(String name, Serializer serializer) {
        RaftWorkQueue workQueue = new RaftWorkQueue(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.WORK_QUEUE.name())
            .withReadConsistency(ReadConsistency.LINEARIZABLE_LEASE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withTimeout(Duration.ofSeconds(5))
            .withMaxRetries(5)
            .build()
            .open()
            .join());
        return new TranscodingAsyncWorkQueue<>(workQueue, serializer::encode, serializer::decode);
    }

    @Override
    public <V> AsyncDocumentTree<V> newAsyncDocumentTree(String name, Serializer serializer, Ordering ordering) {
        RaftDocumentTree documentTree = new RaftDocumentTree(client.newProxyBuilder()
            .withName(name)
            .withServiceType(String.format("%s-%s", DistributedPrimitive.Type.DOCUMENT_TREE.name(), ordering))
            .withReadConsistency(ReadConsistency.SEQUENTIAL)
            .withCommunicationStrategy(CommunicationStrategy.ANY)
            .withTimeout(Duration.ofSeconds(30))
            .withMaxRetries(5)
            .build()
            .open()
            .join());
        return new TranscodingAsyncDocumentTree<>(documentTree, serializer::encode, serializer::decode);
    }

    @Override
    public <T> AsyncLeaderElector<T> newAsyncLeaderElector(String name, Serializer serializer, Duration electionTimeout) {
        RaftLeaderElector leaderElector = new RaftLeaderElector(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.LEADER_ELECTOR.name())
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withMinTimeout(electionTimeout)
            .withMaxTimeout(Duration.ofSeconds(5))
            .withMaxRetries(5)
            .build()
            .open()
            .join());
        return new TranscodingAsyncLeaderElector<>(leaderElector, serializer::encode, serializer::decode);
    }

    @Override
    public AsyncDistributedLock newAsyncDistributedLock(String name, Duration lockTimeout) {
        return new RaftDistributedLock(client.newProxyBuilder()
            .withName(name)
            .withServiceType(DistributedPrimitive.Type.LOCK.name())
            .withReadConsistency(ReadConsistency.LINEARIZABLE)
            .withCommunicationStrategy(CommunicationStrategy.LEADER)
            .withMinTimeout(lockTimeout)
            .withMaxTimeout(Duration.ofSeconds(5))
            .withMaxRetries(5)
            .build()
            .open()
            .join());
    }

    @Override
    public Set<String> getPrimitiveNames(DistributedPrimitive.Type primitiveType) {
        return client.metadata().getSessions(primitiveType.name())
            .join()
            .stream()
            .map(RaftSessionMetadata::serviceName)
            .collect(Collectors.toSet());
    }

}

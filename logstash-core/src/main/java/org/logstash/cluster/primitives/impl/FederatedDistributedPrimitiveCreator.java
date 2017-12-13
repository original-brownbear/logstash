package org.logstash.cluster.primitives.impl;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import org.logstash.cluster.primitives.DistributedPrimitive;
import org.logstash.cluster.primitives.DistributedPrimitiveCreator;
import org.logstash.cluster.primitives.DistributedPrimitives;
import org.logstash.cluster.primitives.Hasher;
import org.logstash.cluster.primitives.Ordering;
import org.logstash.cluster.primitives.counter.AsyncAtomicCounter;
import org.logstash.cluster.primitives.generator.AsyncAtomicIdGenerator;
import org.logstash.cluster.primitives.leadership.AsyncLeaderElector;
import org.logstash.cluster.primitives.lock.AsyncDistributedLock;
import org.logstash.cluster.primitives.map.AsyncAtomicCounterMap;
import org.logstash.cluster.primitives.map.AsyncConsistentMap;
import org.logstash.cluster.primitives.map.AsyncConsistentTreeMap;
import org.logstash.cluster.primitives.map.impl.PartitionedAsyncConsistentMap;
import org.logstash.cluster.primitives.multimap.AsyncConsistentMultimap;
import org.logstash.cluster.primitives.queue.AsyncWorkQueue;
import org.logstash.cluster.primitives.set.AsyncDistributedSet;
import org.logstash.cluster.primitives.tree.AsyncDocumentTree;
import org.logstash.cluster.primitives.tree.DocumentPath;
import org.logstash.cluster.primitives.tree.impl.PartitionedAsyncDocumentTree;
import org.logstash.cluster.primitives.value.AsyncAtomicValue;
import org.logstash.cluster.serializer.Serializer;

/**
 * {@code DistributedPrimitiveCreator} that federates responsibility for creating
 * distributed primitives to a collection of other {@link DistributedPrimitiveCreator creators}.
 */
public class FederatedDistributedPrimitiveCreator implements DistributedPrimitiveCreator {

    private static final Funnel<Iterable<? extends CharSequence>> STR_LIST_FUNNEL =
        Funnels.sequentialFunnel(Funnels.unencodedCharsFunnel());

    private final TreeMap<Integer, DistributedPrimitiveCreator> members;
    private final List<Integer> sortedMemberPartitionIds;
    private final int buckets;

    public FederatedDistributedPrimitiveCreator(final Map<Integer, DistributedPrimitiveCreator> members, final int buckets) {
        this.members = Maps.newTreeMap();
        this.members.putAll(Preconditions.checkNotNull(members));
        this.sortedMemberPartitionIds = Lists.newArrayList(members.keySet());
        this.buckets = buckets;
    }

    @Override
    public <K, V> AsyncConsistentMap<K, V> newAsyncConsistentMap(final String name, final Serializer serializer) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(serializer);
        final Map<Integer, AsyncConsistentMap<byte[], byte[]>> maps =
            Maps.transformValues(members,
                partition -> DistributedPrimitives.newTranscodingMap(
                    partition.<String, byte[]>newAsyncConsistentMap(name, null),
                    BaseEncoding.base16()::encode,
                    BaseEncoding.base16()::decode,
                    Function.identity(),
                    Function.identity()));
        final Hasher<byte[]> hasher = key -> {
            final int bucket = Math.abs(Hashing.murmur3_32().hashBytes(key).asInt()) % buckets;
            return sortedMemberPartitionIds.get(Hashing.consistentHash(bucket, sortedMemberPartitionIds.size()));
        };
        final AsyncConsistentMap<byte[], byte[]> partitionedMap = new PartitionedAsyncConsistentMap<>(name, maps, hasher);
        return DistributedPrimitives.newTranscodingMap(partitionedMap,
            serializer::encode,
            serializer::decode,
            value -> value == null ? null : serializer.encode(value),
            serializer::decode);
    }

    @Override
    public <K, V> AsyncConsistentTreeMap<K, V> newAsyncConsistentTreeMap(final String name, final Serializer serializer) {
        return getCreator(name).newAsyncConsistentTreeMap(name, serializer);
    }

    @Override
    public <K, V> AsyncConsistentMultimap<K, V> newAsyncConsistentSetMultimap(final String name, final Serializer serializer) {
        return getCreator(name).newAsyncConsistentSetMultimap(name, serializer);
    }

    @Override
    public <E> AsyncDistributedSet<E> newAsyncDistributedSet(final String name, final Serializer serializer) {
        return DistributedPrimitives.newSetFromMap(newAsyncConsistentMap(name, serializer));
    }

    @Override
    public <K> AsyncAtomicCounterMap<K> newAsyncAtomicCounterMap(final String name, final Serializer serializer) {
        return getCreator(name).newAsyncAtomicCounterMap(name, serializer);
    }

    @Override
    public AsyncAtomicCounter newAsyncCounter(final String name) {
        return getCreator(name).newAsyncCounter(name);
    }

    @Override
    public AsyncAtomicIdGenerator newAsyncIdGenerator(final String name) {
        return getCreator(name).newAsyncIdGenerator(name);
    }

    @Override
    public <V> AsyncAtomicValue<V> newAsyncAtomicValue(final String name, final Serializer serializer) {
        return getCreator(name).newAsyncAtomicValue(name, serializer);
    }

    @Override
    public <T> AsyncLeaderElector<T> newAsyncLeaderElector(final String name, final Serializer serializer, final Duration electionTimeout) {
        return getCreator(name).newAsyncLeaderElector(name, serializer, electionTimeout);
    }

    @Override
    public AsyncDistributedLock newAsyncDistributedLock(final String name, final Duration timeout) {
        return getCreator(name).newAsyncDistributedLock(name, timeout);
    }

    @Override
    public <E> AsyncWorkQueue<E> newAsyncWorkQueue(final String name, final Serializer serializer) {
        return getCreator(name).newAsyncWorkQueue(name, serializer);
    }

    @Override
    public <V> AsyncDocumentTree<V> newAsyncDocumentTree(final String name, final Serializer serializer, final Ordering ordering) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(serializer);
        final Map<Integer, AsyncDocumentTree<V>> trees =
            Maps.transformValues(members, part -> part.<V>newAsyncDocumentTree(name, serializer, ordering));
        final Hasher<DocumentPath> hasher = key -> {
            final int bucket = key == null ? 0 :
                Math.abs(Hashing.murmur3_32()
                    .hashObject(key.pathElements(), STR_LIST_FUNNEL)
                    .asInt()) % buckets;
            return sortedMemberPartitionIds.get(Hashing.consistentHash(bucket, sortedMemberPartitionIds.size()));
        };
        return new PartitionedAsyncDocumentTree<>(name, trees, hasher);
    }

    @Override
    public Set<String> getPrimitiveNames(final DistributedPrimitive.Type primitiveType) {
        return members.values()
            .stream()
            .map(m -> m.getPrimitiveNames(primitiveType))
            .reduce(Sets::union)
            .orElse(ImmutableSet.of());
    }

    /**
     * Returns the {@code DistributedPrimitiveCreator} to use for hosting a primitive.
     * @param name primitive name
     * @return primitive creator
     */
    private DistributedPrimitiveCreator getCreator(final String name) {
        final int hashCode = Hashing.sha256().hashString(name, Charsets.UTF_8).asInt();
        return members.get(sortedMemberPartitionIds.get(Math.abs(hashCode) % members.size()));
    }
}

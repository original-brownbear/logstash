package org.logstash.cluster.primitives;

import java.util.Set;
import org.logstash.cluster.primitives.counter.AtomicCounterBuilder;
import org.logstash.cluster.primitives.generator.AtomicIdGeneratorBuilder;
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

/**
 * Primitive service.
 */
public interface PrimitiveService {

    /**
     * Creates a new ConsistentMapBuilder.
     * @param <K> key type
     * @param <V> value type
     * @return builder for a consistent map
     */
    <K, V> ConsistentMapBuilder<K, V> consistentMapBuilder();

    /**
     * Creates a new ConsistentMapBuilder.
     * @param <V> value type
     * @return builder for a consistent map
     */
    <V> DocumentTreeBuilder<V> documentTreeBuilder();

    /**
     * Creates a new {@code AsyncConsistentTreeMapBuilder}.
     * @param <K> key type
     * @param <V> value type
     * @return builder for a async consistent tree map
     */
    <K, V> ConsistentTreeMapBuilder<K, V> consistentTreeMapBuilder();

    /**
     * Creates a new {@code AsyncConsistentSetMultimapBuilder}.
     * @param <K> key type
     * @param <V> value type
     * @return builder for a set based async consistent multimap
     */
    <K, V> ConsistentMultimapBuilder<K, V> consistentMultimapBuilder();

    /**
     * Creates a new {@code AtomicCounterMapBuilder}.
     * @param <K> key type
     * @return builder for an atomic counter map
     */
    <K> AtomicCounterMapBuilder<K> atomicCounterMapBuilder();

    /**
     * Creates a new DistributedSetBuilder.
     * @param <E> set element type
     * @return builder for an distributed set
     */
    <E> DistributedSetBuilder<E> setBuilder();

    /**
     * Creates a new AtomicCounterBuilder.
     * @return atomic counter builder
     */
    AtomicCounterBuilder atomicCounterBuilder();

    /**
     * Creates a new AtomicIdGeneratorBuilder.
     * @return atomic ID generator builder
     */
    AtomicIdGeneratorBuilder atomicIdGeneratorBuilder();

    /**
     * Creates a new AtomicValueBuilder.
     * @param <V> atomic value type
     * @return atomic value builder
     */
    <V> AtomicValueBuilder<V> atomicValueBuilder();

    /**
     * Creates a new LeaderElectorBuilder.
     * @return leader elector builder
     */
    <T> LeaderElectorBuilder<T> leaderElectorBuilder();

    /**
     * Creates a new DistributedLockBuilder.
     * @return distributed lock builder
     */
    DistributedLockBuilder lockBuilder();

    /**
     * Creates a new WorkQueueBuilder.
     * @param <E> work queue element type
     * @return work queue builder
     */
    <E> WorkQueueBuilder<E> workQueueBuilder();

    /**
     * Returns a list of map names.
     * @return a list of map names
     */
    default Set<String> getConsistentMapNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.CONSISTENT_MAP);
    }

    /**
     * Returns a set of primitive names for the given primitive type.
     * @param primitiveType the primitive type for which to return names
     * @return a set of names of the given primitive type
     */
    Set<String> getPrimitiveNames(DistributedPrimitive.Type primitiveType);

    /**
     * Returns a list of document tree names.
     * @return a list of document tree names
     */
    default Set<String> getDocumentTreeNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.DOCUMENT_TREE);
    }

    /**
     * Returns a list of tree map names.
     * @return a list of tree map names
     */
    default Set<String> getConsistentTreeMapNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.CONSISTENT_TREEMAP);
    }

    /**
     * Returns a list of multimap names.
     * @return a list of multimap names
     */
    default Set<String> getConsistentMultimapNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.CONSISTENT_MULTIMAP);
    }

    /**
     * Returns a list of counter map names.
     * @return a list of counter map names
     */
    default Set<String> getAtomicCounterMapNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.COUNTER_MAP);
    }

    /**
     * Returns a list of set names.
     * @return a list of set names
     */
    default Set<String> getSetNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.SET);
    }

    /**
     * Returns a list of counter names.
     * @return a list of counter names
     */
    default Set<String> getAtomicCounterNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.COUNTER);
    }

    /**
     * Returns a list of ID generator names.
     * @return a list of ID generator names
     */
    default Set<String> getAtomicIdGeneratorNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.ID_GENERATOR);
    }

    /**
     * Returns a list of atomic value names.
     * @return a list of atomic value names
     */
    default Set<String> getAtomicValueNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.VALUE);
    }

    /**
     * Returns a list of leader elector names.
     * @return a list of leader elector names
     */
    default Set<String> getLeaderElectorNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.LEADER_ELECTOR);
    }

    /**
     * Returns a list of lock names.
     * @return a list of lock names
     */
    default Set<String> getDistributedLockNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.LOCK);
    }

    /**
     * Returns a list of work queue names.
     * @return a list of work queue names
     */
    default Set<String> getWorkQueueNames() {
        return getPrimitiveNames(DistributedPrimitive.Type.WORK_QUEUE);
    }

}
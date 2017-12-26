package org.logstash.cluster.primitives.map.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.AbstractMap;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.Match;

/**
 * {@link org.logstash.cluster.primitives.map.AsyncConsistentTreeMap} Resource
 * state machine operations.
 */
public enum RaftConsistentTreeMapOperations implements OperationId {
    SUB_MAP("subMap", OperationType.QUERY),
    FIRST_KEY("firstKey", OperationType.QUERY),
    LAST_KEY("lastKey", OperationType.QUERY),
    FIRST_ENTRY("firstEntry", OperationType.QUERY),
    LAST_ENTRY("lastEntry", OperationType.QUERY),
    POLL_FIRST_ENTRY("pollFirstEntry", OperationType.QUERY),
    POLL_LAST_ENTRY("pollLastEntry", OperationType.QUERY),
    LOWER_ENTRY("lowerEntry", OperationType.QUERY),
    LOWER_KEY("lowerKey", OperationType.QUERY),
    FLOOR_ENTRY("floorEntry", OperationType.QUERY),
    FLOOR_KEY("floorKey", OperationType.QUERY),
    CEILING_ENTRY("ceilingEntry", OperationType.QUERY),
    CEILING_KEY("ceilingKey", OperationType.QUERY),
    HIGHER_ENTRY("higherEntry", OperationType.QUERY),
    HIGHER_KEY("higherKey", OperationType.QUERY);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID + 100)
        .register(RaftConsistentTreeMapOperations.LowerKey.class)
        .register(RaftConsistentTreeMapOperations.LowerEntry.class)
        .register(RaftConsistentTreeMapOperations.HigherKey.class)
        .register(RaftConsistentTreeMapOperations.HigherEntry.class)
        .register(RaftConsistentTreeMapOperations.FloorKey.class)
        .register(RaftConsistentTreeMapOperations.FloorEntry.class)
        .register(RaftConsistentTreeMapOperations.CeilingKey.class)
        .register(RaftConsistentTreeMapOperations.CeilingEntry.class)
        .register(Versioned.class)
        .register(AbstractMap.SimpleImmutableEntry.class)
        .register(Maps.immutableEntry("", "").getClass())
        .build(RaftConsistentTreeMapOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftConsistentTreeMapOperations(String id, OperationType type) {
        this.id = id;
        this.type = type;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public OperationType type() {
        return type;
    }

    /**
     * Abstract treeMap command.
     */
    @SuppressWarnings("serial")
    public abstract static class TreeOperation {
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .toString();
        }
    }

    /**
     * Abstract key-based query.
     */
    @SuppressWarnings("serial")
    public abstract static class KeyOperation extends RaftConsistentTreeMapOperations.TreeOperation {
        protected String key;

        public KeyOperation(String key) {
            this.key = Preconditions.checkNotNull(key);
        }

        public KeyOperation() {
        }

        public String key() {
            return key;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("key", key)
                .toString();
        }
    }

    /**
     * Abstract value-based query.
     */
    @SuppressWarnings("serial")
    public abstract static class ValueOperation extends RaftConsistentTreeMapOperations.TreeOperation {
        protected byte[] value;

        public ValueOperation() {
        }

        public ValueOperation(byte[] value) {
            this.value = Preconditions.checkNotNull(value);
        }

        public byte[] value() {
            return value;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("value", value)
                .toString();
        }
    }

    /**
     * Contains key command.
     */
    @SuppressWarnings("serial")
    public static class ContainsKey extends RaftConsistentTreeMapOperations.KeyOperation {

        public ContainsKey(String key) {
            super(key);
        }

        public ContainsKey() {
        }
    }

    /**
     * Contains value command.
     */
    @SuppressWarnings("serial")
    public static class ContainsValue extends RaftConsistentTreeMapOperations.ValueOperation {
        public ContainsValue() {
        }

        public ContainsValue(byte[] value) {
            super(value);
        }

    }

    /**
     * AsyncConsistentTreeMap update command.
     */
    @SuppressWarnings("serial")
    public static class UpdateAndGet extends RaftConsistentTreeMapOperations.TreeOperation {
        private String key;
        private byte[] value;
        private Match<byte[]> valueMatch;
        private Match<Long> versionMatch;

        public UpdateAndGet() {
        }

        public UpdateAndGet(String key,
            byte[] value,
            Match<byte[]> valueMatch,
            Match<Long> versionMatch) {
            this.key = key;
            this.value = value;
            this.valueMatch = valueMatch;
            this.versionMatch = versionMatch;
        }

        public String key() {
            return this.key;
        }

        public byte[] value() {
            return this.value;
        }

        public Match<byte[]> valueMatch() {
            return this.valueMatch;
        }

        public Match<Long> versionMatch() {
            return this.versionMatch;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("key", key)
                .add("value", value)
                .add("valueMatch", valueMatch)
                .add("versionMatch", versionMatch)
                .toString();
        }
    }

    /**
     * Get query.
     */
    @SuppressWarnings("serial")
    public static class Get extends RaftConsistentTreeMapOperations.KeyOperation {
        public Get() {
        }

        public Get(String key) {
            super(key);
        }
    }

    /**
     * Get or default query.
     */
    @SuppressWarnings("serial")
    public static class GetOrDefault extends RaftConsistentTreeMapOperations.KeyOperation {
        private byte[] defaultValue;

        public GetOrDefault() {
        }

        public GetOrDefault(String key, byte[] defaultValue) {
            super(key);
            this.defaultValue = defaultValue;
        }

        /**
         * Returns the default value.
         * @return the default value
         */
        public byte[] defaultValue() {
            return defaultValue;
        }
    }

    /**
     * Query returns the entry associated with the largest key less than the
     * passed in key.
     */
    @SuppressWarnings("serial")
    public static class LowerEntry extends RaftConsistentTreeMapOperations.KeyOperation {
        public LowerEntry() {
        }

        public LowerEntry(String key) {
            super(key);
        }
    }

    /**
     * Query returns the largest key less than the specified key.
     */
    @SuppressWarnings("serial")
    public static class LowerKey extends RaftConsistentTreeMapOperations.KeyOperation {
        public LowerKey() {
        }

        public LowerKey(String key) {
            super(key);
        }
    }

    /**
     * Query returns the entry associated with the largest key smaller than or
     * equal to the specified key.
     */
    @SuppressWarnings("serial")
    public static class FloorEntry extends RaftConsistentTreeMapOperations.KeyOperation {
        public FloorEntry() {
        }

        public FloorEntry(String key) {
            super(key);
        }
    }

    /**
     * Query returns the largest key smaller than or equal to the passed in
     * key.
     */
    @SuppressWarnings("serial")
    public static class FloorKey extends RaftConsistentTreeMapOperations.KeyOperation {
        public FloorKey() {
        }

        public FloorKey(String key) {
            super(key);
        }
    }

    /**
     * Returns the entry associated with the smallest key larger than or equal
     * to the specified key.
     */
    @SuppressWarnings("serial")
    public static class CeilingEntry extends RaftConsistentTreeMapOperations.KeyOperation {
        public CeilingEntry() {
        }

        public CeilingEntry(String key) {
            super(key);
        }
    }

    /**
     * Returns the smallest key larger than or equal to the specified key.
     */
    @SuppressWarnings("serial")
    public static class CeilingKey extends RaftConsistentTreeMapOperations.KeyOperation {
        public CeilingKey() {
        }

        public CeilingKey(String key) {
            super(key);
        }
    }

    /**
     * Returns the entry associated with the smallest key larger than the
     * specified key.
     */
    @SuppressWarnings("serial")
    public static class HigherEntry extends RaftConsistentTreeMapOperations.KeyOperation {
        public HigherEntry() {
        }

        public HigherEntry(String key) {
            super(key);
        }
    }

    /**
     * Returns the smallest key larger than the specified key.
     */
    @SuppressWarnings("serial")
    public static class HigherKey extends RaftConsistentTreeMapOperations.KeyOperation {
        public HigherKey() {
        }

        public HigherKey(String key) {
            super(key);
        }
    }

    @SuppressWarnings("serial")
    public static class SubMap<K, V> extends RaftConsistentTreeMapOperations.TreeOperation {
        private K fromKey;
        private K toKey;
        private boolean inclusiveFrom;
        private boolean inclusiveTo;

        public SubMap() {
        }

        public SubMap(K fromKey, K toKey, boolean inclusiveFrom,
            boolean inclusiveTo) {
            this.fromKey = fromKey;
            this.toKey = toKey;
            this.inclusiveFrom = inclusiveFrom;
            this.inclusiveTo = inclusiveTo;
        }

        public K fromKey() {
            return fromKey;
        }        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("getFromKey", fromKey)
                .add("getToKey", toKey)
                .add("inclusiveFrotBound", inclusiveFrom)
                .add("inclusiveToBound", inclusiveTo)
                .toString();
        }

        public K toKey() {
            return toKey;
        }

        public boolean isInclusiveFrom() {
            return inclusiveFrom;
        }

        public boolean isInclusiveTo() {
            return inclusiveTo;
        }


    }
}

package org.logstash.cluster.primitives.map.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.Set;
import org.logstash.cluster.primitives.TransactionId;
import org.logstash.cluster.primitives.TransactionLog;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.time.Versioned;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * {@link RaftConsistentMap} resource state machine operations.
 */
public enum RaftConsistentMapOperations implements OperationId {
    IS_EMPTY("isEmpty", OperationType.QUERY),
    SIZE("size", OperationType.QUERY),
    CONTAINS_KEY("containsKey", OperationType.QUERY),
    CONTAINS_VALUE("containsValue", OperationType.QUERY),
    GET("get", OperationType.QUERY),
    GET_ALL_PRESENT("getAllPresent", OperationType.QUERY),
    GET_OR_DEFAULT("getOrDefault", OperationType.QUERY),
    KEY_SET("keySet", OperationType.QUERY),
    VALUES("values", OperationType.QUERY),
    ENTRY_SET("entrySet", OperationType.QUERY),
    PUT("put", OperationType.COMMAND),
    PUT_IF_ABSENT("putIfAbsent", OperationType.COMMAND),
    PUT_AND_GET("putAndGet", OperationType.COMMAND),
    REMOVE("remove", OperationType.COMMAND),
    REMOVE_VALUE("removeValue", OperationType.COMMAND),
    REMOVE_VERSION("removeVersion", OperationType.COMMAND),
    REPLACE("replace", OperationType.COMMAND),
    REPLACE_VALUE("replaceValue", OperationType.COMMAND),
    REPLACE_VERSION("replaceVersion", OperationType.COMMAND),
    CLEAR("clear", OperationType.COMMAND),
    ADD_LISTENER("addListener", OperationType.COMMAND),
    REMOVE_LISTENER("removeListener", OperationType.COMMAND),
    BEGIN("begin", OperationType.COMMAND),
    PREPARE("prepare", OperationType.COMMAND),
    PREPARE_AND_COMMIT("prepareAndCommit", OperationType.COMMAND),
    COMMIT("commit", OperationType.COMMAND),
    ROLLBACK("rollback", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(RaftConsistentMapOperations.ContainsKey.class)
        .register(RaftConsistentMapOperations.ContainsValue.class)
        .register(RaftConsistentMapOperations.Get.class)
        .register(RaftConsistentMapOperations.GetAllPresent.class)
        .register(RaftConsistentMapOperations.GetOrDefault.class)
        .register(RaftConsistentMapOperations.Put.class)
        .register(RaftConsistentMapOperations.Remove.class)
        .register(RaftConsistentMapOperations.RemoveValue.class)
        .register(RaftConsistentMapOperations.RemoveVersion.class)
        .register(RaftConsistentMapOperations.Replace.class)
        .register(RaftConsistentMapOperations.ReplaceValue.class)
        .register(RaftConsistentMapOperations.ReplaceVersion.class)
        .register(RaftConsistentMapOperations.TransactionBegin.class)
        .register(RaftConsistentMapOperations.TransactionPrepare.class)
        .register(RaftConsistentMapOperations.TransactionPrepareAndCommit.class)
        .register(RaftConsistentMapOperations.TransactionCommit.class)
        .register(RaftConsistentMapOperations.TransactionRollback.class)
        .register(TransactionId.class)
        .register(TransactionLog.class)
        .register(MapUpdate.class)
        .register(MapUpdate.Type.class)
        .register(PrepareResult.class)
        .register(CommitResult.class)
        .register(RollbackResult.class)
        .register(MapEntryUpdateResult.class)
        .register(MapEntryUpdateResult.Status.class)
        .register(Versioned.class)
        .register(byte[].class)
        .build(RaftConsistentMapOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftConsistentMapOperations(String id, OperationType type) {
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
     * Abstract map command.
     */
    @SuppressWarnings("serial")
    public abstract static class MapOperation {
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
    public abstract static class KeyOperation extends RaftConsistentMapOperations.MapOperation {
        protected String key;

        public KeyOperation() {
        }

        public KeyOperation(String key) {
            this.key = Preconditions.checkNotNull(key, "key cannot be null");
        }

        /**
         * Returns the key.
         * @return key
         */
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
    public abstract static class ValueOperation extends RaftConsistentMapOperations.MapOperation {
        protected byte[] value;

        public ValueOperation() {
        }

        public ValueOperation(byte[] value) {
            this.value = value;
        }

        /**
         * Returns the value.
         * @return value
         */
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
     * Abstract key/value operation.
     */
    @SuppressWarnings("serial")
    public abstract static class KeyValueOperation extends RaftConsistentMapOperations.KeyOperation {
        protected byte[] value;

        public KeyValueOperation() {
        }

        public KeyValueOperation(String key, byte[] value) {
            super(key);
            this.value = value;
        }

        /**
         * Returns the value.
         * @return value
         */
        public byte[] value() {
            return value;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("key", key)
                .add("value", ArraySizeHashPrinter.of(value))
                .toString();
        }
    }

    /**
     * Abstract key/version operation.
     */
    @SuppressWarnings("serial")
    public abstract static class KeyVersionOperation extends RaftConsistentMapOperations.KeyOperation {
        protected long version;

        public KeyVersionOperation() {
        }

        public KeyVersionOperation(String key, long version) {
            super(key);
            this.version = version;
        }

        /**
         * Returns the version.
         * @return version
         */
        public long version() {
            return version;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("key", key)
                .add("version", version)
                .toString();
        }
    }

    /**
     * Contains key command.
     */
    @SuppressWarnings("serial")
    public static class ContainsKey extends RaftConsistentMapOperations.KeyOperation {
        public ContainsKey() {
        }

        public ContainsKey(String key) {
            super(key);
        }
    }

    /**
     * Contains value command.
     */
    @SuppressWarnings("serial")
    public static class ContainsValue extends RaftConsistentMapOperations.ValueOperation {
        public ContainsValue() {
        }

        public ContainsValue(byte[] value) {
            super(value);
        }
    }

    /**
     * Map put operation.
     */
    public static class Put extends RaftConsistentMapOperations.KeyValueOperation {
        public Put() {
        }

        public Put(String key, byte[] value) {
            super(key, value);
        }
    }

    /**
     * Remove operation.
     */
    public static class Remove extends RaftConsistentMapOperations.KeyOperation {
        public Remove() {
        }

        public Remove(String key) {
            super(key);
        }
    }

    /**
     * Remove if value match operation.
     */
    public static class RemoveValue extends RaftConsistentMapOperations.KeyValueOperation {
        public RemoveValue() {
        }

        public RemoveValue(String key, byte[] value) {
            super(key, value);
        }
    }

    /**
     * Remove if version match operation.
     */
    public static class RemoveVersion extends RaftConsistentMapOperations.KeyVersionOperation {
        public RemoveVersion() {
        }

        public RemoveVersion(String key, long version) {
            super(key, version);
        }
    }

    /**
     * Replace operation.
     */
    public static class Replace extends RaftConsistentMapOperations.KeyValueOperation {
        public Replace() {
        }

        public Replace(String key, byte[] value) {
            super(key, value);
        }
    }

    /**
     * Replace by value operation.
     */
    public static class ReplaceValue extends RaftConsistentMapOperations.KeyOperation {
        private byte[] oldValue;
        private byte[] newValue;

        public ReplaceValue() {
        }

        public ReplaceValue(String key, byte[] oldValue, byte[] newValue) {
            super(key);
            this.oldValue = oldValue;
            this.newValue = newValue;
        }

        public byte[] oldValue() {
            return oldValue;
        }

        public byte[] newValue() {
            return newValue;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("oldValue", ArraySizeHashPrinter.of(oldValue))
                .add("newValue", ArraySizeHashPrinter.of(newValue))
                .toString();
        }
    }

    /**
     * Replace by version operation.
     */
    public static class ReplaceVersion extends RaftConsistentMapOperations.KeyOperation {
        private long oldVersion;
        private byte[] newValue;

        public ReplaceVersion() {
        }

        public ReplaceVersion(String key, long oldVersion, byte[] newValue) {
            super(key);
            this.oldVersion = oldVersion;
            this.newValue = newValue;
        }

        public long oldVersion() {
            return oldVersion;
        }

        public byte[] newValue() {
            return newValue;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("oldVersion", oldVersion)
                .add("newValue", ArraySizeHashPrinter.of(newValue))
                .toString();
        }
    }

    /**
     * Transaction begin command.
     */
    public static class TransactionBegin extends RaftConsistentMapOperations.MapOperation {
        private TransactionId transactionId;

        public TransactionBegin() {
        }

        public TransactionBegin(TransactionId transactionId) {
            this.transactionId = transactionId;
        }

        public TransactionId transactionId() {
            return transactionId;
        }
    }

    /**
     * Map prepare command.
     */
    @SuppressWarnings("serial")
    public static class TransactionPrepare extends RaftConsistentMapOperations.MapOperation {
        private TransactionLog<MapUpdate<String, byte[]>> transactionLog;

        public TransactionPrepare() {
        }

        public TransactionPrepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
            this.transactionLog = transactionLog;
        }

        public TransactionLog<MapUpdate<String, byte[]>> transactionLog() {
            return transactionLog;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("transactionLog", transactionLog)
                .toString();
        }
    }

    /**
     * Map prepareAndCommit command.
     */
    @SuppressWarnings("serial")
    public static class TransactionPrepareAndCommit extends RaftConsistentMapOperations.TransactionPrepare {
        public TransactionPrepareAndCommit() {
        }

        public TransactionPrepareAndCommit(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
            super(transactionLog);
        }
    }

    /**
     * Map transaction commit command.
     */
    @SuppressWarnings("serial")
    public static class TransactionCommit extends RaftConsistentMapOperations.MapOperation {
        private TransactionId transactionId;

        public TransactionCommit() {
        }

        public TransactionCommit(TransactionId transactionId) {
            this.transactionId = transactionId;
        }

        /**
         * Returns the transaction identifier.
         * @return transaction id
         */
        public TransactionId transactionId() {
            return transactionId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("transactionId", transactionId)
                .toString();
        }
    }

    /**
     * Map transaction rollback command.
     */
    @SuppressWarnings("serial")
    public static class TransactionRollback extends RaftConsistentMapOperations.MapOperation {
        private TransactionId transactionId;

        public TransactionRollback() {
        }

        public TransactionRollback(TransactionId transactionId) {
            this.transactionId = transactionId;
        }

        /**
         * Returns the transaction identifier.
         * @return transaction id
         */
        public TransactionId transactionId() {
            return transactionId;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("transactionId", transactionId)
                .toString();
        }
    }

    /**
     * Get query.
     */
    @SuppressWarnings("serial")
    public static class Get extends RaftConsistentMapOperations.KeyOperation {
        public Get() {
        }

        public Get(String key) {
            super(key);
        }
    }

    /**
     * Get all present query.
     */
    @SuppressWarnings("serial")
    public static class GetAllPresent extends RaftConsistentMapOperations.MapOperation {
        private Set<String> keys;

        public GetAllPresent() {
        }

        public GetAllPresent(Set<String> keys) {
            this.keys = keys;
        }

        /**
         * Returns the keys.
         * @return the keys
         */
        public Set<String> keys() {
            return keys;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("keys", keys)
                .toString();
        }
    }

    /**
     * Get or default query.
     */
    @SuppressWarnings("serial")
    public static class GetOrDefault extends RaftConsistentMapOperations.KeyOperation {
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

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("defaultValue", ArraySizeHashPrinter.of(defaultValue))
                .toString();
        }
    }
}

package org.logstash.cluster.primitives.value.impl;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * Counter commands.
 */
public enum RaftAtomicValueOperations implements OperationId {
    GET("get", OperationType.QUERY),
    SET("set", OperationType.COMMAND),
    COMPARE_AND_SET("compareAndSet", OperationType.COMMAND),
    GET_AND_SET("getAndSet", OperationType.COMMAND),
    ADD_LISTENER("addListener", OperationType.COMMAND),
    REMOVE_LISTENER("removeListener", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(RaftAtomicValueOperations.Get.class)
        .register(RaftAtomicValueOperations.Set.class)
        .register(RaftAtomicValueOperations.CompareAndSet.class)
        .register(RaftAtomicValueOperations.GetAndSet.class)
        .build(RaftAtomicValueOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftAtomicValueOperations(String id, OperationType type) {
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
     * Abstract value command.
     */
    public abstract static class ValueOperation {
    }

    /**
     * Get query.
     */
    public static class Get extends RaftAtomicValueOperations.ValueOperation {
    }

    /**
     * Set command.
     */
    public static class Set extends RaftAtomicValueOperations.ValueOperation {
        private byte[] value;

        public Set() {
        }

        public Set(byte[] value) {
            this.value = value;
        }

        /**
         * Returns the command value.
         * @return The command value.
         */
        public byte[] value() {
            return value;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("value", ArraySizeHashPrinter.of(value))
                .toString();
        }
    }

    /**
     * Compare and set command.
     */
    public static class CompareAndSet extends RaftAtomicValueOperations.ValueOperation {
        private byte[] expect;
        private byte[] update;

        public CompareAndSet() {
        }

        public CompareAndSet(byte[] expect, byte[] update) {
            this.expect = expect;
            this.update = update;
        }

        /**
         * Returns the expected value.
         * @return The expected value.
         */
        public byte[] expect() {
            return expect;
        }

        /**
         * Returns the updated value.
         * @return The updated value.
         */
        public byte[] update() {
            return update;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("expect", ArraySizeHashPrinter.of(expect))
                .add("update", ArraySizeHashPrinter.of(update))
                .toString();
        }
    }

    /**
     * Get and set operation.
     */
    public static class GetAndSet extends RaftAtomicValueOperations.ValueOperation {
        private byte[] value;

        public GetAndSet() {
        }

        public GetAndSet(byte[] value) {
            this.value = value;
        }

        /**
         * Returns the command value.
         * @return The command value.
         */
        public byte[] value() {
            return value;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("value", ArraySizeHashPrinter.of(value))
                .toString();
        }
    }
}

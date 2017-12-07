package org.logstash.cluster.primitives.counter.impl;

import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Counter commands.
 */
public enum RaftAtomicCounterOperations implements OperationId {
    SET("set", OperationType.COMMAND),
    COMPARE_AND_SET("compareAndSet", OperationType.COMMAND),
    INCREMENT_AND_GET("incrementAndGet", OperationType.COMMAND),
    GET_AND_INCREMENT("getAndIncrement", OperationType.COMMAND),
    ADD_AND_GET("addAndGet", OperationType.COMMAND),
    GET_AND_ADD("getAndAdd", OperationType.COMMAND),
    GET("get", OperationType.QUERY);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(RaftAtomicCounterOperations.Get.class)
        .register(RaftAtomicCounterOperations.Set.class)
        .register(RaftAtomicCounterOperations.CompareAndSet.class)
        .register(RaftAtomicCounterOperations.AddAndGet.class)
        .register(RaftAtomicCounterOperations.GetAndAdd.class)
        .build(RaftAtomicCounterOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftAtomicCounterOperations(String id, OperationType type) {
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
    public static class Get extends RaftAtomicCounterOperations.ValueOperation {
    }

    /**
     * Set command.
     */
    public static class Set extends RaftAtomicCounterOperations.ValueOperation {
        private Long value;

        public Set() {
        }

        public Set(Long value) {
            this.value = value;
        }

        /**
         * Returns the command value.
         * @return The command value.
         */
        public Long value() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("%s[value=%s]", getClass().getSimpleName(), value);
        }
    }

    /**
     * Compare and set command.
     */
    public static class CompareAndSet extends RaftAtomicCounterOperations.ValueOperation {
        private Long expect;
        private Long update;

        public CompareAndSet() {
        }

        public CompareAndSet(Long expect, Long update) {
            this.expect = expect;
            this.update = update;
        }

        /**
         * Returns the expected value.
         * @return The expected value.
         */
        public Long expect() {
            return expect;
        }

        /**
         * Returns the updated value.
         * @return The updated value.
         */
        public Long update() {
            return update;
        }

        @Override
        public String toString() {
            return String.format("%s[expect=%s, update=%s]", getClass().getSimpleName(), expect, update);
        }
    }

    /**
     * Delta command.
     */
    public abstract static class DeltaOperation extends RaftAtomicCounterOperations.ValueOperation {
        private long delta;

        public DeltaOperation() {
        }

        public DeltaOperation(long delta) {
            this.delta = delta;
        }

        /**
         * Returns the delta.
         * @return The delta.
         */
        public long delta() {
            return delta;
        }
    }

    /**
     * Get and add command.
     */
    public static class GetAndAdd extends RaftAtomicCounterOperations.DeltaOperation {
        public GetAndAdd() {
        }

        public GetAndAdd(long delta) {
            super(delta);
        }
    }

    /**
     * Add and get command.
     */
    public static class AddAndGet extends RaftAtomicCounterOperations.DeltaOperation {
        public AddAndGet() {
        }

        public AddAndGet(long delta) {
            super(delta);
        }
    }
}

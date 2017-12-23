package org.logstash.cluster.primitives.map.impl;

import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Atomic counter map commands.
 */
public enum RaftAtomicCounterMapOperations implements OperationId {
    PUT("put", OperationType.COMMAND),
    PUT_IF_ABSENT("putIfAbsent", OperationType.COMMAND),
    GET("get", OperationType.QUERY),
    REPLACE("replace", OperationType.COMMAND),
    REMOVE("remove", OperationType.COMMAND),
    REMOVE_VALUE("removeValue", OperationType.COMMAND),
    GET_AND_INCREMENT("getAndIncrement", OperationType.COMMAND),
    GET_AND_DECREMENT("getAndDecrement", OperationType.COMMAND),
    INCREMENT_AND_GET("incrementAndGet", OperationType.COMMAND),
    DECREMENT_AND_GET("decrementAndGet", OperationType.COMMAND),
    ADD_AND_GET("addAndGet", OperationType.COMMAND),
    GET_AND_ADD("getAndAdd", OperationType.COMMAND),
    SIZE("size", OperationType.QUERY),
    IS_EMPTY("isEmpty", OperationType.QUERY),
    CLEAR("clear", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(RaftAtomicCounterMapOperations.IncrementAndGet.class)
        .register(RaftAtomicCounterMapOperations.DecrementAndGet.class)
        .register(RaftAtomicCounterMapOperations.GetAndIncrement.class)
        .register(RaftAtomicCounterMapOperations.GetAndDecrement.class)
        .register(RaftAtomicCounterMapOperations.AddAndGet.class)
        .register(RaftAtomicCounterMapOperations.GetAndAdd.class)
        .register(RaftAtomicCounterMapOperations.Get.class)
        .register(RaftAtomicCounterMapOperations.Put.class)
        .register(RaftAtomicCounterMapOperations.PutIfAbsent.class)
        .register(RaftAtomicCounterMapOperations.Replace.class)
        .register(RaftAtomicCounterMapOperations.Remove.class)
        .register(RaftAtomicCounterMapOperations.RemoveValue.class)
        .build(RaftAtomicCounterMapOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftAtomicCounterMapOperations(String id, OperationType type) {
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

    public abstract static class AtomicCounterMapOperation<V> {
    }

    public abstract static class KeyOperation extends RaftAtomicCounterMapOperations.AtomicCounterMapOperation {
        private String key;

        public KeyOperation() {
        }

        public KeyOperation(String key) {
            this.key = key;
        }

        public String key() {
            return key;
        }
    }

    public static class KeyValueOperation extends RaftAtomicCounterMapOperations.KeyOperation {
        private long value;

        public KeyValueOperation() {
        }

        public KeyValueOperation(String key, long value) {
            super(key);
            this.value = value;
        }

        public long value() {
            return value;
        }
    }

    public static class Get extends RaftAtomicCounterMapOperations.KeyOperation {
        public Get() {
        }

        public Get(String key) {
            super(key);
        }
    }

    public static class Put extends RaftAtomicCounterMapOperations.KeyValueOperation {
        public Put() {
        }

        public Put(String key, long value) {
            super(key, value);
        }
    }

    public static class PutIfAbsent extends RaftAtomicCounterMapOperations.KeyValueOperation {
        public PutIfAbsent() {
        }

        public PutIfAbsent(String key, long value) {
            super(key, value);
        }
    }

    public static class Replace extends RaftAtomicCounterMapOperations.KeyOperation {
        private long replace;
        private long value;

        public Replace() {
        }

        public Replace(String key, long replace, long value) {
            super(key);
            this.replace = replace;
            this.value = value;
        }

        public long replace() {
            return replace;
        }

        public long value() {
            return value;
        }
    }

    public static class Remove extends RaftAtomicCounterMapOperations.KeyOperation {
        public Remove() {
        }

        public Remove(String key) {
            super(key);
        }
    }

    public static class RemoveValue extends RaftAtomicCounterMapOperations.KeyValueOperation {
        public RemoveValue() {
        }

        public RemoveValue(String key, long value) {
            super(key, value);
        }
    }

    public static class IncrementAndGet extends RaftAtomicCounterMapOperations.KeyOperation {
        public IncrementAndGet() {
        }

        public IncrementAndGet(String key) {
            super(key);
        }
    }

    public static class DecrementAndGet extends RaftAtomicCounterMapOperations.KeyOperation {
        public DecrementAndGet(String key) {
            super(key);
        }

        public DecrementAndGet() {
        }
    }

    public static class GetAndIncrement extends RaftAtomicCounterMapOperations.KeyOperation {
        public GetAndIncrement() {
        }

        public GetAndIncrement(String key) {
            super(key);
        }
    }

    public static class GetAndDecrement extends RaftAtomicCounterMapOperations.KeyOperation {
        public GetAndDecrement() {
        }

        public GetAndDecrement(String key) {
            super(key);
        }
    }

    public abstract static class DeltaOperation extends RaftAtomicCounterMapOperations.KeyOperation {
        private long delta;

        public DeltaOperation() {
        }

        public DeltaOperation(String key, long delta) {
            super(key);
            this.delta = delta;
        }

        public long delta() {
            return delta;
        }
    }

    public static class AddAndGet extends RaftAtomicCounterMapOperations.DeltaOperation {
        public AddAndGet() {
        }

        public AddAndGet(String key, long delta) {
            super(key, delta);
        }
    }

    public static class GetAndAdd extends RaftAtomicCounterMapOperations.DeltaOperation {
        public GetAndAdd() {
        }

        public GetAndAdd(String key, long delta) {
            super(key, delta);
        }
    }
}

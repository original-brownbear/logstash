package org.logstash.cluster.primitives.lock.impl;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * Counter commands.
 */
public enum RaftDistributedLockOperations implements OperationId {
    LOCK("lock", OperationType.COMMAND),
    UNLOCK("unlock", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(Lock.class)
        .register(Unlock.class)
        .build(RaftDistributedLockOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftDistributedLockOperations(String id, OperationType type) {
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
     * Abstract lock operation.
     */
    public abstract static class LockOperation {
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).toString();
        }
    }

    /**
     * Lock command.
     */
    public static class Lock extends LockOperation {
        private final int id;
        private final long timeout;

        public Lock() {
            this(0, 0);
        }

        public Lock(int id, long timeout) {
            this.id = id;
            this.timeout = timeout;
        }

        /**
         * Returns the lock identifier.
         * @return the lock identifier
         */
        public int id() {
            return id;
        }

        /**
         * Returns the lock attempt timeout.
         * @return the lock attempt timeout
         */
        public long timeout() {
            return timeout;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("timeout", timeout)
                .toString();
        }
    }

    /**
     * Unlock command.
     */
    public static class Unlock extends LockOperation {
        private final int id;

        public Unlock(int id) {
            this.id = id;
        }

        /**
         * Returns the lock identifier.
         * @return the lock identifier
         */
        public int id() {
            return id;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("id", id)
                .toString();
        }
    }
}

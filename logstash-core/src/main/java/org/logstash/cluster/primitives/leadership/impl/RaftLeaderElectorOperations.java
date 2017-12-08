package org.logstash.cluster.primitives.leadership.impl;

import com.google.common.base.MoreObjects;
import org.logstash.cluster.cluster.NodeId;
import org.logstash.cluster.primitives.leadership.Leader;
import org.logstash.cluster.primitives.leadership.Leadership;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;
import org.logstash.cluster.utils.ArraySizeHashPrinter;

/**
 * {@link RaftLeaderElector} resource state machine operations.
 */
public enum RaftLeaderElectorOperations implements OperationId {
    ADD_LISTENER("addListener", OperationType.COMMAND),
    REMOVE_LISTENER("removeListener", OperationType.COMMAND),
    RUN("run", OperationType.COMMAND),
    WITHDRAW("withdraw", OperationType.COMMAND),
    ANOINT("anoint", OperationType.COMMAND),
    PROMOTE("promote", OperationType.COMMAND),
    EVICT("evict", OperationType.COMMAND),
    GET_LEADERSHIP("getLeadership", OperationType.QUERY);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(NodeId.class)
        .register(Leadership.class)
        .register(Leader.class)
        .register(RaftLeaderElectorOperations.Run.class)
        .register(RaftLeaderElectorOperations.Withdraw.class)
        .register(RaftLeaderElectorOperations.Anoint.class)
        .register(RaftLeaderElectorOperations.Promote.class)
        .register(RaftLeaderElectorOperations.Evict.class)
        .build(RaftLeaderElectorOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftLeaderElectorOperations(String id, OperationType type) {
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
     * Abstract election operation.
     */
    @SuppressWarnings("serial")
    public abstract static class ElectionOperation {
    }

    /**
     * Election operation that uses an instance identifier.
     */
    public abstract static class ElectionChangeOperation extends RaftLeaderElectorOperations.ElectionOperation {
        private byte[] id;

        public ElectionChangeOperation() {
        }

        public ElectionChangeOperation(byte[] id) {
            this.id = id;
        }

        /**
         * Returns the candidate identifier.
         * @return the candidate identifier
         */
        public byte[] id() {
            return id;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("id", ArraySizeHashPrinter.of(id))
                .toString();
        }
    }

    /**
     * Enter and run for leadership.
     */
    @SuppressWarnings("serial")
    public static class Run extends RaftLeaderElectorOperations.ElectionChangeOperation {
        public Run() {
        }

        public Run(byte[] id) {
            super(id);
        }
    }

    /**
     * Command for withdrawing a candidate from an election.
     */
    @SuppressWarnings("serial")
    public static class Withdraw extends RaftLeaderElectorOperations.ElectionChangeOperation {
        private Withdraw() {
        }

        public Withdraw(byte[] id) {
            super(id);
        }
    }

    /**
     * Command for administratively anoint a node as leader.
     */
    @SuppressWarnings("serial")
    public static class Anoint extends RaftLeaderElectorOperations.ElectionChangeOperation {
        private Anoint() {
        }

        public Anoint(byte[] id) {
            super(id);
        }
    }

    /**
     * Command for administratively promote a node as top candidate.
     */
    @SuppressWarnings("serial")
    public static class Promote extends RaftLeaderElectorOperations.ElectionChangeOperation {
        private Promote() {
        }

        public Promote(byte[] id) {
            super(id);
        }
    }

    /**
     * Command for administratively evicting a node from all leadership topics.
     */
    @SuppressWarnings("serial")
    public static class Evict extends RaftLeaderElectorOperations.ElectionChangeOperation {
        public Evict() {
        }

        public Evict(byte[] id) {
            super(id);
        }
    }
}
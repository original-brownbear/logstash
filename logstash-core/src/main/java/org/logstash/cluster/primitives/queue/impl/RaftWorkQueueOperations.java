package org.logstash.cluster.primitives.queue.impl;

import com.google.common.base.MoreObjects;
import java.util.Collection;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueueStats;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * {@link RaftWorkQueue} resource state machine operations.
 */
public enum RaftWorkQueueOperations implements OperationId {
    STATS("stats", OperationType.QUERY),
    REGISTER("register", OperationType.COMMAND),
    UNREGISTER("unregister", OperationType.COMMAND),
    ADD("add", OperationType.COMMAND),
    TAKE("take", OperationType.COMMAND),
    COMPLETE("complete", OperationType.COMMAND),
    CLEAR("clear", OperationType.COMMAND);

    public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
        .register(RaftWorkQueueOperations.Add.class)
        .register(RaftWorkQueueOperations.Take.class)
        .register(RaftWorkQueueOperations.Complete.class)
        .register(Task.class)
        .register(WorkQueueStats.class)
        .build(RaftWorkQueueOperations.class.getSimpleName());
    private final String id;
    private final OperationType type;

    RaftWorkQueueOperations(String id, OperationType type) {
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
     * Work queue operation.
     */
    public abstract static class WorkQueueOperation {
    }

    /**
     * Command to add a collection of tasks to the queue.
     */
    @SuppressWarnings("serial")
    public static class Add extends RaftWorkQueueOperations.WorkQueueOperation {
        private Collection<byte[]> items;

        private Add() {
        }

        public Add(Collection<byte[]> items) {
            this.items = items;
        }

        public Collection<byte[]> items() {
            return items;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("items", items)
                .toString();
        }
    }

    /**
     * Command to take a task from the queue.
     */
    @SuppressWarnings("serial")
    public static class Take extends RaftWorkQueueOperations.WorkQueueOperation {
        private int maxTasks;

        private Take() {
        }

        public Take(int maxTasks) {
            this.maxTasks = maxTasks;
        }

        public int maxTasks() {
            return maxTasks;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("maxTasks", maxTasks)
                .toString();
        }
    }

    @SuppressWarnings("serial")
    public static class Complete extends RaftWorkQueueOperations.WorkQueueOperation {
        private Collection<String> taskIds;

        private Complete() {
        }

        public Complete(Collection<String> taskIds) {
            this.taskIds = taskIds;
        }

        public Collection<String> taskIds() {
            return taskIds;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("taskIds", taskIds)
                .toString();
        }
    }
}

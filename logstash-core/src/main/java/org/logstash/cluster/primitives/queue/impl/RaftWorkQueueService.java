package org.logstash.cluster.primitives.queue.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.logstash.cluster.primitives.queue.Task;
import org.logstash.cluster.primitives.queue.WorkQueueStats;
import org.logstash.cluster.protocols.raft.service.AbstractRaftService;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.session.RaftSession;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotReader;
import org.logstash.cluster.protocols.raft.storage.snapshot.SnapshotWriter;
import org.logstash.cluster.serializer.Serializer;
import org.logstash.cluster.serializer.kryo.KryoNamespace;
import org.logstash.cluster.serializer.kryo.KryoNamespaces;

/**
 * State machine for {@link RaftWorkQueue} resource.
 */
public class RaftWorkQueueService extends AbstractRaftService {

    private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
        .register(KryoNamespaces.BASIC)
        .register(RaftWorkQueueOperations.NAMESPACE)
        .register(RaftWorkQueueEvents.NAMESPACE)
        .register(RaftWorkQueueService.TaskAssignment.class)
        .register(new HashMap().keySet().getClass())
        .register(ArrayDeque.class)
        .build());

    private final AtomicLong totalCompleted = new AtomicLong(0);

    private Queue<Task<byte[]>> unassignedTasks = Queues.newArrayDeque();
    private Map<String, RaftWorkQueueService.TaskAssignment> assignments = Maps.newHashMap();
    private Map<Long, RaftSession> registeredWorkers = Maps.newHashMap();

    @Override
    public void snapshot(final SnapshotWriter writer) {
        writer.writeObject(Sets.newHashSet(registeredWorkers.keySet()), SERIALIZER::encode);
        writer.writeObject(assignments, SERIALIZER::encode);
        writer.writeObject(unassignedTasks, SERIALIZER::encode);
        writer.writeLong(totalCompleted.get());
    }

    @Override
    public void install(final SnapshotReader reader) {
        registeredWorkers = Maps.newHashMap();
        for (final Long sessionId : reader.<Set<Long>>readObject(SERIALIZER::decode)) {
            registeredWorkers.put(sessionId, sessions().getSession(sessionId));
        }
        assignments = reader.readObject(SERIALIZER::decode);
        unassignedTasks = reader.readObject(SERIALIZER::decode);
        totalCompleted.set(reader.readLong());
    }

    @Override
    protected void configure(final RaftServiceExecutor executor) {
        executor.register(RaftWorkQueueOperations.STATS, this::stats, SERIALIZER::encode);
        executor.register(RaftWorkQueueOperations.REGISTER, this::register);
        executor.register(RaftWorkQueueOperations.UNREGISTER, this::unregister);
        executor.register(RaftWorkQueueOperations.ADD, SERIALIZER::decode, this::add);
        executor.register(RaftWorkQueueOperations.TAKE, SERIALIZER::decode, this::take, SERIALIZER::encode);
        executor.register(RaftWorkQueueOperations.COMPLETE, SERIALIZER::decode, this::complete);
        executor.register(RaftWorkQueueOperations.CLEAR, this::clear);
    }

    @Override
    public void onExpire(final RaftSession session) {
        evictWorker(session.sessionId().id());
    }

    @Override
    public void onClose(final RaftSession session) {
        evictWorker(session.sessionId().id());
    }

    private void evictWorker(final long sessionId) {
        registeredWorkers.remove(sessionId);

        // TODO: Maintain an index of tasks by session for efficient access.
        final Iterator<Map.Entry<String, RaftWorkQueueService.TaskAssignment>> iter = assignments.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<String, RaftWorkQueueService.TaskAssignment> entry = iter.next();
            final RaftWorkQueueService.TaskAssignment assignment = entry.getValue();
            if (assignment.sessionId() == sessionId) {
                unassignedTasks.add(assignment.task());
                iter.remove();
            }
        }
    }

    protected WorkQueueStats stats(final Commit<Void> commit) {
        return WorkQueueStats.builder()
            .withTotalCompleted(totalCompleted.get())
            .withTotalPending(unassignedTasks.size())
            .withTotalInProgress(assignments.size())
            .build();
    }

    protected void clear(final Commit<Void> commit) {
        unassignedTasks.clear();
        assignments.clear();
        registeredWorkers.clear();
        totalCompleted.set(0);
    }

    protected void register(final Commit<Void> commit) {
        registeredWorkers.put(commit.session().sessionId().id(), commit.session());
    }

    protected void unregister(final Commit<Void> commit) {
        registeredWorkers.remove(commit.session().sessionId().id());
    }

    protected void add(final Commit<? extends RaftWorkQueueOperations.Add> commit) {
        final Collection<byte[]> items = commit.value().items();

        final AtomicInteger itemIndex = new AtomicInteger(0);
        items.forEach(item -> {
            final String taskId = String.format("%d:%d:%d", commit.session().sessionId().id(),
                commit.index(),
                itemIndex.getAndIncrement());
            unassignedTasks.add(new Task<>(taskId, item));
        });

        // Send an event to all sessions that have expressed interest in task processing
        // and are not actively processing a task.
        registeredWorkers.values().forEach(session -> session.publish(RaftWorkQueueEvents.TASK_AVAILABLE));
        // FIXME: This generates a lot of event traffic.
    }

    protected Collection<Task<byte[]>> take(final Commit<? extends RaftWorkQueueOperations.Take> commit) {
        try {
            if (unassignedTasks.isEmpty()) {
                return ImmutableList.of();
            }
            final long sessionId = commit.session().sessionId().id();
            final int maxTasks = commit.value().maxTasks();
            return IntStream.range(0, Math.min(maxTasks, unassignedTasks.size()))
                .mapToObj(i -> {
                    Task<byte[]> task = unassignedTasks.poll();
                    String taskId = task.taskId();
                    RaftWorkQueueService.TaskAssignment assignment = new RaftWorkQueueService.TaskAssignment(sessionId, task);

                    // bookkeeping
                    assignments.put(taskId, assignment);

                    return task;
                })
                .collect(Collectors.toCollection(ArrayList::new));
        } catch (final Exception e) {
            logger().warn("State machine update failed", e);
            throw Throwables.propagate(e);
        }
    }

    protected void complete(final Commit<? extends RaftWorkQueueOperations.Complete> commit) {
        final long sessionId = commit.session().sessionId().id();
        try {
            commit.value().taskIds().forEach(taskId -> {
                final RaftWorkQueueService.TaskAssignment assignment = assignments.get(taskId);
                if (assignment != null && assignment.sessionId() == sessionId) {
                    assignments.remove(taskId);
                    // bookkeeping
                    totalCompleted.incrementAndGet();
                }
            });
        } catch (final Exception e) {
            logger().warn("State machine update failed", e);
            throw Throwables.propagate(e);
        }
    }

    private static class TaskAssignment {
        private final long sessionId;
        private final Task<byte[]> task;

        public TaskAssignment(final long sessionId, final Task<byte[]> task) {
            this.sessionId = sessionId;
            this.task = task;
        }

        public long sessionId() {
            return sessionId;
        }

        public Task<byte[]> task() {
            return task;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(getClass())
                .add("sessionId", sessionId)
                .add("task", task)
                .toString();
        }
    }
}

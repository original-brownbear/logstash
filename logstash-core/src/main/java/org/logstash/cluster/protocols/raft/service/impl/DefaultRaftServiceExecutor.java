package org.logstash.cluster.protocols.raft.service.impl;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.cluster.protocols.raft.RaftException;
import org.logstash.cluster.protocols.raft.operation.OperationId;
import org.logstash.cluster.protocols.raft.operation.OperationType;
import org.logstash.cluster.protocols.raft.service.Commit;
import org.logstash.cluster.protocols.raft.service.RaftServiceExecutor;
import org.logstash.cluster.protocols.raft.service.ServiceContext;
import org.logstash.cluster.time.WallClockTimestamp;
import org.logstash.cluster.utils.concurrent.Scheduled;

/**
 * Default operation executor.
 */
public class DefaultRaftServiceExecutor implements RaftServiceExecutor {
    private static final Logger LOGGER = LogManager.getLogger(DefaultRaftServiceExecutor.class);
    private final Queue<Runnable> tasks = new LinkedList<>();
    private final List<DefaultRaftServiceExecutor.ScheduledTask> scheduledTasks = new ArrayList<>();
    private final List<DefaultRaftServiceExecutor.ScheduledTask> complete = new ArrayList<>();
    private final Map<OperationId, Function<Commit<byte[]>, byte[]>> operations = new HashMap<>();
    private OperationType operationType;
    private long timestamp;

    public DefaultRaftServiceExecutor(ServiceContext context) {

    }

    @Override
    public void tick(WallClockTimestamp timestamp) {
        long unixTimestamp = timestamp.unixTimestamp();
        if (!scheduledTasks.isEmpty()) {
            // Iterate through scheduled tasks until we reach a task that has not met its scheduled time.
            // The tasks list is sorted by time on insertion.
            Iterator<DefaultRaftServiceExecutor.ScheduledTask> iterator = scheduledTasks.iterator();
            while (iterator.hasNext()) {
                DefaultRaftServiceExecutor.ScheduledTask task = iterator.next();
                if (task.isRunnable(unixTimestamp)) {
                    this.timestamp = task.time;
                    this.operationType = OperationType.COMMAND;
                    LOGGER.trace("Executing scheduled task {}", task);
                    task.execute();
                    complete.add(task);
                    iterator.remove();
                } else {
                    break;
                }
            }

            // Iterate through tasks that were completed and reschedule them.
            for (DefaultRaftServiceExecutor.ScheduledTask task : complete) {
                task.reschedule(this.timestamp);
            }
            complete.clear();
        }
    }

    @Override
    public byte[] apply(Commit<byte[]> commit) {
        LOGGER.trace("Executing {}", commit);

        this.operationType = commit.operation().type();
        this.timestamp = commit.wallClockTime().unixTimestamp();

        // Look up the registered callback for the operation.
        Function<Commit<byte[]>, byte[]> callback = operations.get(commit.operation());

        if (callback == null) {
            throw new IllegalStateException("Unknown state machine operation: " + commit.operation());
        } else {
            // Execute the operation. If the operation return value is a Future, await the result,
            // otherwise immediately complete the execution future.
            try {
                return callback.apply(commit);
            } catch (Exception e) {
                LOGGER.warn("State machine operation failed: {}", e.getMessage());
                throw new RaftException.ApplicationException(e);
            } finally {
                runTasks();
            }
        }
    }

    @Override
    public void handle(OperationId operationId, Function<Commit<byte[]>, byte[]> callback) {
        Preconditions.checkNotNull(operationId, "operationId cannot be null");
        Preconditions.checkNotNull(callback, "callback cannot be null");
        operations.put(operationId, callback);
        LOGGER.debug("Registered operation callback {}", operationId);
    }

    /**
     * Executes tasks after an operation.
     */
    private void runTasks() {
        // Execute any tasks that were queue during execution of the command.
        if (!tasks.isEmpty()) {
            for (Runnable task : tasks) {
                LOGGER.trace("Executing task {}", task);
                task.run();
            }
            tasks.clear();
        }
    }

    @Override
    public void execute(Runnable callback) {
        checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
        tasks.add(callback);
    }

    /**
     * Checks that the current operation is of the given type.
     * @param type the operation type
     * @param message the message to print if the current operation does not match the given type
     */
    private void checkOperation(OperationType type, String message) {
        Preconditions.checkState(operationType == type, message);
    }

    @Override
    public Scheduled schedule(Duration delay, Runnable callback) {
        checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
        LOGGER.trace("Scheduled callback {} with delay {}", callback, delay);
        return new DefaultRaftServiceExecutor.ScheduledTask(callback, delay.toMillis()).schedule();
    }

    @Override
    public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
        checkOperation(OperationType.COMMAND, "callbacks can only be scheduled during command execution");
        LOGGER.trace("Scheduled repeating callback {} with initial delay {} and interval {}", callback, initialDelay, interval);
        return new DefaultRaftServiceExecutor.ScheduledTask(callback, initialDelay.toMillis(), interval.toMillis()).schedule();
    }

    /**
     * Scheduled task.
     */
    private class ScheduledTask implements Scheduled {
        private final long interval;
        private final Runnable callback;
        private long time;

        private ScheduledTask(Runnable callback, long delay) {
            this(callback, delay, 0);
        }

        private ScheduledTask(Runnable callback, long delay, long interval) {
            this.interval = interval;
            this.callback = callback;
            this.time = timestamp + delay;
        }

        /**
         * Reschedules the task.
         */
        private void reschedule(long timestamp) {
            if (interval > 0) {
                time = timestamp + interval;
                schedule();
            }
        }

        /**
         * Schedules the task.
         */
        private Scheduled schedule() {
            // Perform binary search to insert the task at the appropriate position in the tasks list.
            if (scheduledTasks.isEmpty()) {
                scheduledTasks.add(this);
            } else {
                int l = 0;
                int u = scheduledTasks.size() - 1;
                int i;
                while (true) {
                    i = (u + l) / 2;
                    long t = scheduledTasks.get(i).time;
                    if (t == time) {
                        scheduledTasks.add(i, this);
                        return this;
                    } else if (t < time) {
                        l = i + 1;
                        if (l > u) {
                            scheduledTasks.add(i + 1, this);
                            return this;
                        }
                    } else {
                        u = i - 1;
                        if (l > u) {
                            scheduledTasks.add(i, this);
                            return this;
                        }
                    }
                }
            }
            return this;
        }

        /**
         * Returns a boolean value indicating whether the task delay has been met.
         */
        private boolean isRunnable(long timestamp) {
            return timestamp > time;
        }

        /**
         * Executes the task.
         */
        private synchronized void execute() {
            callback.run();
        }

        @Override
        public synchronized void cancel() {
            scheduledTasks.remove(this);
        }
    }
}

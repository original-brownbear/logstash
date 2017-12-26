package org.logstash.cluster.primitives.queue;

import com.google.common.base.MoreObjects;

/**
 * Statistics for a {@link AsyncWorkQueue}.
 */
public final class WorkQueueStats {

    private long totalPending;
    private long totalInProgress;
    private long totalCompleted;

    private WorkQueueStats() {
    }

    /**
     * Returns a {@code WorkQueueStats} builder.
     * @return builder
     */
    public static WorkQueueStats.Builder builder() {
        return new WorkQueueStats.Builder();
    }

    /**
     * Returns the total pending tasks. These are the tasks that are added but not yet picked up.
     * @return total pending tasks.
     */
    public long totalPending() {
        return this.totalPending;
    }

    /**
     * Returns the total in progress tasks. These are the tasks that are currently being worked on.
     * @return total in progress tasks.
     */
    public long totalInProgress() {
        return this.totalInProgress;
    }

    /**
     * Returns the total completed tasks.
     * @return total completed tasks.
     */
    public long totalCompleted() {
        return this.totalCompleted;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("totalPending", totalPending)
            .add("totalInProgress", totalInProgress)
            .add("totalCompleted", totalCompleted)
            .toString();
    }

    public static class Builder {

        final WorkQueueStats workQueueStats = new WorkQueueStats();

        public WorkQueueStats.Builder withTotalPending(long value) {
            workQueueStats.totalPending = value;
            return this;
        }

        public WorkQueueStats.Builder withTotalInProgress(long value) {
            workQueueStats.totalInProgress = value;
            return this;
        }

        public WorkQueueStats.Builder withTotalCompleted(long value) {
            workQueueStats.totalCompleted = value;
            return this;
        }

        public WorkQueueStats build() {
            return workQueueStats;
        }
    }
}

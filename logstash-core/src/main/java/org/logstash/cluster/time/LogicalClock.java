package org.logstash.cluster.time;

import com.google.common.base.MoreObjects;

/**
 * Logical clock.
 */
public class LogicalClock implements Clock<LogicalTimestamp> {
    private LogicalTimestamp currentTimestamp;

    public LogicalClock() {
        this(new LogicalTimestamp(0));
    }

    public LogicalClock(LogicalTimestamp currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
    }

    /**
     * Increments the clock and updates it using the given timestamp.
     * @param timestamp the timestamp with which to update the clock
     * @return the updated clock time
     */
    public LogicalTimestamp incrementAndUpdate(LogicalTimestamp timestamp) {
        long nextValue = currentTimestamp.value() + 1;
        if (timestamp.value() > nextValue) {
            return update(timestamp);
        }
        return increment();
    }

    /**
     * Increments the clock and returns the new timestamp.
     * @return the updated clock time
     */
    public LogicalTimestamp increment() {
        return update(new LogicalTimestamp(currentTimestamp.value() + 1));
    }

    /**
     * Updates the clock using the given timestamp.
     * @param timestamp the timestamp with which to update the clock
     * @return the updated clock time
     */
    public LogicalTimestamp update(LogicalTimestamp timestamp) {
        if (timestamp.value() > currentTimestamp.value()) {
            this.currentTimestamp = timestamp;
        }
        return currentTimestamp;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("time", getTime())
            .toString();
    }

    @Override
    public LogicalTimestamp getTime() {
        return currentTimestamp;
    }
}

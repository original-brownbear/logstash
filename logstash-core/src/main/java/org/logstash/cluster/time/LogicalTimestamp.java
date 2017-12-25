package org.logstash.cluster.time;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import java.util.Objects;

/**
 * Timestamp based on logical sequence value.
 * <p>
 * LogicalTimestamps are ordered by their sequence values.
 */
public class LogicalTimestamp implements Timestamp {

    private final long value;

    public LogicalTimestamp(long value) {
        this.value = value;
    }

    /**
     * Returns a new logical timestamp for the given logical time.
     * @param value the logical time for which to create a new logical timestamp
     * @return the logical timestamp
     */
    public static LogicalTimestamp of(long value) {
        return new LogicalTimestamp(value);
    }

    /**
     * Returns the sequence value.
     * @return sequence value
     */
    public long value() {
        return this.value;
    }

    /**
     * Returns the timestamp as a version.
     * @return the timestamp as a version
     */
    public Version asVersion() {
        return new Version(value);
    }

    @Override
    public int compareTo(Timestamp o) {
        Preconditions.checkArgument(o instanceof LogicalTimestamp,
            "Must be LogicalTimestamp", o);
        LogicalTimestamp that = (LogicalTimestamp) o;

        return ComparisonChain.start()
            .compare(this.value, that.value)
            .result();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LogicalTimestamp)) {
            return false;
        }
        LogicalTimestamp that = (LogicalTimestamp) obj;
        return Objects.equals(this.value, that.value);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
            .add("value", value)
            .toString();
    }
}

package org.logstash.cluster.time;

import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import java.util.Objects;
import org.logstash.cluster.utils.TimestampPrinter;

/**
 * A Timestamp that derives its value from the prevailing
 * wallclock time on the controller where it is generated.
 */
public class WallClockTimestamp implements Timestamp {

    private final long unixTimestamp;

    public WallClockTimestamp() {
        unixTimestamp = System.currentTimeMillis();
    }

    public WallClockTimestamp(long timestamp) {
        unixTimestamp = timestamp;
    }

    /**
     * Returns a new wall clock timestamp for the given unix timestamp.
     * @param unixTimestamp the unix timestamp for which to create a new wall clock timestamp
     * @return the wall clock timestamp
     */
    public static WallClockTimestamp from(long unixTimestamp) {
        return new WallClockTimestamp(unixTimestamp);
    }

    @Override
    public int compareTo(Timestamp o) {
        Preconditions.checkArgument(o instanceof WallClockTimestamp,
            "Must be WallClockTimestamp", o);
        WallClockTimestamp that = (WallClockTimestamp) o;

        return ComparisonChain.start()
            .compare(this.unixTimestamp, that.unixTimestamp)
            .result();
    }

    @Override
    public int hashCode() {
        return Long.hashCode(unixTimestamp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof WallClockTimestamp)) {
            return false;
        }
        WallClockTimestamp that = (WallClockTimestamp) obj;
        return Objects.equals(this.unixTimestamp, that.unixTimestamp);
    }

    @Override
    public String toString() {
        return new TimestampPrinter(unixTimestamp).toString();
    }

    /**
     * Returns the unixTimestamp.
     * @return unix timestamp
     */
    public long unixTimestamp() {
        return unixTimestamp;
    }
}

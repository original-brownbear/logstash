package org.logstash.cluster.time;

import com.google.common.base.MoreObjects;

/**
 * Wall clock.
 */
public class WallClock implements Clock<WallClockTimestamp> {
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("time", getTime())
            .toString();
    }

    @Override
    public WallClockTimestamp getTime() {
        return new WallClockTimestamp();
    }
}

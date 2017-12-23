package org.logstash.cluster.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Timestamp printer.
 */
public class TimestampPrinter {

    private static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss,SSS");
    private final long timestamp;

    public TimestampPrinter(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Returns a new timestamp printer.
     * @param timestamp the timestamp to print
     * @return the timestamp printer
     */
    public static TimestampPrinter of(long timestamp) {
        return new TimestampPrinter(timestamp);
    }

    @Override
    public String toString() {
        return FORMAT.format(new Date(timestamp));
    }
}

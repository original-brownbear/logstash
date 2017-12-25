package org.logstash.cluster.utils;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Timestamp printer test.
 */
public class TimestampPrinterTest {
    @Test
    @Ignore // Timestamp is environment specific
    public void testTimestampPrinter() {
        TimestampPrinter printer = TimestampPrinter.of(1);
        assertEquals("1969-12-31 04:00:00,001", printer.toString());
    }
}

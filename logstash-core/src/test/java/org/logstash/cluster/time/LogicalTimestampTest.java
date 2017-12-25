package org.logstash.cluster.time;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Logical timestamp test.
 */
public class LogicalTimestampTest {
    @Test
    public void testLogicalTimestamp() {
        LogicalTimestamp timestamp = LogicalTimestamp.of(1);
        assertEquals(1, timestamp.value());
        assertTrue(timestamp.isNewerThan(LogicalTimestamp.of(0)));
        assertFalse(timestamp.isNewerThan(LogicalTimestamp.of(2)));
        assertTrue(timestamp.isOlderThan(LogicalTimestamp.of(2)));
        assertFalse(timestamp.isOlderThan(LogicalTimestamp.of(0)));
    }
}

package org.logstash.cluster.time;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Logical clock test.
 */
public class LogicalClockTest {
    @Test
    public void testLogicalClock() {
        LogicalClock clock = new LogicalClock();
        assertEquals(1, clock.increment().value());
        assertEquals(1, clock.getTime().value());
        assertEquals(2, clock.increment().value());
        assertEquals(2, clock.getTime().value());
        assertEquals(5, clock.update(LogicalTimestamp.of(5)).value());
        assertEquals(5, clock.getTime().value());
        assertEquals(5, clock.update(LogicalTimestamp.of(3)).value());
    }
}

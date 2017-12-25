package org.logstash.cluster.time;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Wall clock test.
 */
public class WallClockTest {
    @Test
    public void testWallClock() throws Exception {
        WallClock clock = new WallClock();
        WallClockTimestamp time = clock.getTime();
        assertNotNull(time);
        Thread.sleep(5);
        assertTrue(clock.getTime().unixTimestamp() > time.unixTimestamp());
    }
}

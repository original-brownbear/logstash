package org.logstash.cluster.protocols.raft.session;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Raft session event test.
 */
public class RaftSessionEventTest {
    @Test
    public void testRaftSessionEvent() {
        RaftSession session = mock(RaftSession.class);
        long timestamp = System.currentTimeMillis();
        RaftSessionEvent event = new RaftSessionEvent(RaftSessionEvent.Type.OPEN, session, timestamp);
        assertEquals(RaftSessionEvent.Type.OPEN, event.type());
        assertEquals(session, event.subject());
        assertEquals(timestamp, event.time());
    }
}

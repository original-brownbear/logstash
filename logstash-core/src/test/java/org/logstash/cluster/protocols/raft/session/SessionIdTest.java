package org.logstash.cluster.protocols.raft.session;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Session identifier test.
 */
public class SessionIdTest {
    @Test
    public void testSessionId() {
        SessionId sessionId = SessionId.from(1);
        assertEquals(Long.valueOf(1), sessionId.id());
    }
}
